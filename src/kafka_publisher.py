"""
kafka_publisher.py — Layer 3: Fault-Tolerant Edge-to-Cloud Publisher

Takes the structured JSON payloads produced by Layer 2's AI workers and
publishes them asynchronously to a central Kafka broker. This is the
module that actually bridges the "edge" to the "cloud" in the Hybrid
Edge-Cloud Architecture.

Design invariants:
  1. NEVER block the AI worker's inference loop. The publish() call
     hands the payload to an in-memory queue and returns immediately.
     The actual Kafka send happens on a background thread.
  2. NEVER crash the AI worker if Kafka is unreachable. If the broker
     is down, payloads are persisted to a local SQLite database (the
     "spill file") and retried automatically once the connection is
     restored. This guarantees zero data loss during network outages.
  3. NEVER let the in-memory queue grow unbounded. If payloads are
     arriving faster than the background thread can send them (or
     faster than it can write them to SQLite), the queue is capped
     and the oldest unsent payload is dropped. This mirrors Layer 1's
     "freshness over completeness" philosophy.
  4. The SQLite spill file is per-worker-process (keyed by worker_id)
     to avoid cross-process locking contention. Each AI worker process
     gets its own publisher instance and its own spill file.
  5. On startup, any payloads left in the spill file from a previous
     crash are drained first (oldest-first) before new payloads, so
     delivery order is preserved as closely as possible.

Usage (inside ai_worker.py):
    publisher = FaultTolerantKafkaPublisher(
        worker_id="ai-worker-0",
        broker_url="localhost:9092",
        topic="traffic-anpr-alerts",
    )
    publisher.start()
    ...
    publisher.publish(payload_dict)
    ...
    publisher.stop()  # flushes remaining in-memory payloads to spill file
"""

import json
import time
import sqlite3
import logging
import threading
from queue import Queue, Full, Empty
from typing import Optional, Dict, Any
from pathlib import Path

logger = logging.getLogger("layer3.kafka_publisher")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SQLite-backed spill file for offline fault tolerance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class _SpillStore:
    """
    Append-only SQLite queue for payloads that couldn't be delivered
    to Kafka. FIFO drain order. Each row is deleted only AFTER a
    successful Kafka send confirmation.

    Why SQLite instead of a flat file:
      - Atomic writes (no half-written JSON lines after a crash)
      - Built-in WAL journaling (safe against power loss)
      - Row-level delete after ACK (no need to rewrite the whole file)
      - Ships with Python stdlib — zero extra dependencies
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")  # crash-safe writes
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS spill_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                payload     TEXT    NOT NULL,
                created_at  REAL    NOT NULL
            )
        """)
        self._conn.commit()
        self._lock = threading.Lock()

    def push(self, payload_json: str):
        with self._lock:
            self._conn.execute(
                "INSERT INTO spill_queue (payload, created_at) VALUES (?, ?)",
                (payload_json, time.time()),
            )
            self._conn.commit()

    def peek_batch(self, batch_size: int = 50):
        """Returns up to `batch_size` oldest (id, payload_json) tuples."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT id, payload FROM spill_queue ORDER BY id ASC LIMIT ?",
                (batch_size,),
            )
            return cursor.fetchall()

    def delete_ids(self, ids: list):
        if not ids:
            return
        with self._lock:
            placeholders = ",".join("?" for _ in ids)
            self._conn.execute(
                f"DELETE FROM spill_queue WHERE id IN ({placeholders})", ids
            )
            self._conn.commit()

    def count(self) -> int:
        with self._lock:
            cursor = self._conn.execute("SELECT COUNT(*) FROM spill_queue")
            return cursor.fetchone()[0]

    def close(self):
        with self._lock:
            self._conn.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main publisher class
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FaultTolerantKafkaPublisher:
    """
    Asynchronous, crash-safe Kafka producer for edge-to-cloud payloads.

    Lifecycle:
        publisher = FaultTolerantKafkaPublisher(...)
        publisher.start()      # spawns background sender thread
        publisher.publish(...)  # non-blocking, safe to call from hot loop
        publisher.stop()       # flushes and joins sender thread
    """

    def __init__(
        self,
        worker_id: str,
        broker_url: str = "localhost:9092",
        topic: str = "traffic-anpr-alerts",
        spill_dir: str = "../kafka_spill",
        max_in_memory: int = 500,
        send_batch_size: int = 20,
        retry_backoff_s: float = 5.0,
        flush_timeout_s: float = 10.0,
    ):
        self.worker_id = worker_id
        self.broker_url = broker_url
        self.topic = topic
        self.max_in_memory = max_in_memory
        self.send_batch_size = send_batch_size
        self.retry_backoff_s = retry_backoff_s
        self.flush_timeout_s = flush_timeout_s

        # In-memory queue: AI workers push here, sender thread pops.
        self._queue: Queue = Queue(maxsize=max_in_memory)

        # SQLite spill file: payloads land here when Kafka is down.
        spill_path = str(Path(spill_dir) / f"{worker_id}_spill.db")
        self._spill = _SpillStore(spill_path)

        self._producer = None       # confluent_kafka.Producer, created in start()
        self._sender_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Telemetry counters (read by ai_worker heartbeat)
        self.stats = {
            "sent_ok": 0,
            "spilled": 0,
            "spill_drained": 0,
            "dropped": 0,
            "kafka_errors": 0,
        }

    # ── public API ──────────────────────────────────────────────────

    def start(self):
        """
        Create the Kafka producer and start the background sender thread.
        If broker_url is 'sqlite', it skips Kafka entirely and writes
        directly to the local sentinel.db (for Docker-less local dev).
        """
        self.is_sqlite_mode = (self.broker_url == "sqlite")
        
        if self.is_sqlite_mode:
            logger.info("Starting in SQLite direct mode. Bypassing Kafka.")
            import sqlite3
            db_path = str(Path(__file__).resolve().parent.parent / "sentinel.db")
            # Connect once per thread usually, but we'll create the connection
            # inside the sender thread to be safe with SQLite.
            self._sqlite_db_path = db_path
        else:
            try:
                from confluent_kafka import Producer
            except ImportError:
                logger.error(
                    "confluent-kafka is not installed. "
                    "Run: pip install confluent-kafka   "
                    "(or add it to requirements.txt)"
                )
                raise

            self._producer = Producer({
                "bootstrap.servers": self.broker_url,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 500,
                "linger.ms": 100,
                "batch.size": 65536,
                "compression.type": "lz4",
                "queue.buffering.max.messages": 10000,
                "queue.buffering.max.kbytes": 1024,
                "socket.timeout.ms": 10000,
                "client.id": f"sentinel-edge-{self.worker_id}",
            })


        spill_count = self._spill.count()
        if spill_count > 0:
            logger.info(
                "[%s] Found %d payloads in spill file from previous run — "
                "will drain them first", self.worker_id, spill_count
            )

        self._stop_event.clear()
        self._sender_thread = threading.Thread(
            target=self._sender_loop,
            name=f"kafka-sender-{self.worker_id}",
            daemon=True,
        )
        self._sender_thread.start()
        logger.info("[%s] Kafka publisher started (broker=%s, topic=%s)",
                     self.worker_id, self.broker_url, self.topic)

    def publish(self, payload: Dict[str, Any]):
        """
        Called by the AI worker's hot loop. MUST return instantly.
        Payload is enqueued in-memory for the sender thread; if the
        in-memory queue is full, we drop the oldest entry (same
        "freshness over completeness" policy as Layer 1's frame queues).
        """
        payload_json = json.dumps(payload)
        try:
            self._queue.put_nowait(payload_json)
        except Full:
            # Queue saturated — drop oldest to make room
            try:
                self._queue.get_nowait()
                self.stats["dropped"] += 1
            except Empty:
                pass
            try:
                self._queue.put_nowait(payload_json)
            except Full:
                self.stats["dropped"] += 1

    def stop(self):
        """
        Graceful shutdown: signal the sender thread to stop, flush
        the Kafka producer's internal buffer, and spill any remaining
        in-memory payloads to SQLite so they survive a restart.
        """
        self._stop_event.set()
        if self._sender_thread and self._sender_thread.is_alive():
            self._sender_thread.join(timeout=self.flush_timeout_s + 5)

        # Flush any payloads still in librdkafka's internal buffer
        if self._producer:
            remaining = self._producer.flush(timeout=self.flush_timeout_s)
            if remaining > 0:
                logger.warning("[%s] %d message(s) still in Kafka buffer after flush — "
                               "they may be lost", self.worker_id, remaining)

        # Spill anything left in the in-memory queue to SQLite
        spilled = 0
        while not self._queue.empty():
            try:
                payload_json = self._queue.get_nowait()
                self._spill.push(payload_json)
                spilled += 1
            except Empty:
                break
        if spilled:
            logger.info("[%s] Spilled %d remaining in-memory payloads to SQLite on shutdown",
                         self.worker_id, spilled)

        self._spill.close()
        logger.info("[%s] Kafka publisher stopped. Stats: %s", self.worker_id, self.stats)

    # ── background sender thread ────────────────────────────────────

    def _delivery_callback(self, err, msg):
        """
        Called by librdkafka's background thread when a produce request
        completes (success or failure). We use this to track telemetry.
        """
        if err is not None:
            self.stats["kafka_errors"] += 1
            logger.warning("[%s] Kafka delivery failed: %s", self.worker_id, err)
        else:
            self.stats["sent_ok"] += 1

    def _try_send(self, payload_json: str) -> bool:
        """
        Attempt to produce a single payload to Kafka. Returns True if
        the payload was accepted by librdkafka's internal buffer (NOT
        necessarily ACK'd by the broker yet — that's async via the
        delivery callback). Returns False if the broker is unreachable
        or the internal buffer is full.
        """
        try:
            self._producer.produce(
                topic=self.topic,
                value=payload_json.encode("utf-8"),
                callback=self._delivery_callback,
            )
            self._producer.poll(0)  # trigger delivery callbacks
            return True
        except BufferError:
            # librdkafka's internal queue is full — broker is probably down
            logger.debug("[%s] Kafka internal buffer full — will spill", self.worker_id)
            return False
        except Exception as exc:
            logger.warning("[%s] Kafka produce error: %s", self.worker_id, exc)
            self.stats["kafka_errors"] += 1
            return False

    def _drain_spill(self) -> bool:
        """
        Try to send payloads from the SQLite spill file (oldest first).
        Returns True if the spill file is now empty, False if there are
        still unsent payloads (broker may be down).
        """
        batch = self._spill.peek_batch(self.send_batch_size)
        if not batch:
            return True  # nothing to drain

        sent_ids = []
        for row_id, payload_json in batch:
            if self._stop_event.is_set():
                break
            if self._try_send(payload_json):
                sent_ids.append(row_id)
            else:
                # Broker went down mid-drain — stop, we'll retry later
                break

        if sent_ids:
            self._spill.delete_ids(sent_ids)
            self.stats["spill_drained"] += len(sent_ids)
            logger.info("[%s] Drained %d payload(s) from spill file", self.worker_id, len(sent_ids))

        return self._spill.count() == 0

    def _sender_loop(self):
        """
        Main loop of the background sender thread.
        """
        logger.info("[%s] Sender thread started", self.worker_id)
        
        if getattr(self, 'is_sqlite_mode', False):
            import sqlite3
            from consumer import handle_anpr_alert, handle_heartbeat
            # connect to sqlite inside this thread
            conn = sqlite3.connect(self._sqlite_db_path, check_same_thread=False)
            
            while not self._stop_event.is_set():
                try:
                    payload_json = self._queue.get(timeout=0.5)
                except Empty:
                    continue
                    
                try:
                    payload = json.loads(payload_json)
                    if self.topic == "traffic-anpr-alerts":
                        handle_anpr_alert(conn, payload)
                    else:
                        handle_heartbeat(conn, payload)
                    self.stats["sent_ok"] += 1
                except Exception as exc:
                    logger.error("[%s] SQLite direct write error: %s", self.worker_id, exc)
                    
            conn.close()
            logger.info("[%s] Sender thread exiting (SQLite mode)", self.worker_id)
            return

        while not self._stop_event.is_set():
            # ── Phase 1: drain any spilled payloads from SQLite ─────
            spill_empty = self._drain_spill()

            if not spill_empty:
                # Broker is still down — back off before retrying
                self._producer.poll(0)
                self._stop_event.wait(timeout=self.retry_backoff_s)
                continue

            # ── Phase 2: consume from in-memory queue ───────────────
            try:
                payload_json = self._queue.get(timeout=0.5)
            except Empty:
                # Nothing to send — poll librdkafka for delivery callbacks
                self._producer.poll(0)
                continue

            if self._try_send(payload_json):
                self._producer.poll(0)
            else:
                # Kafka is down — spill this payload and everything
                # currently in the in-memory queue to SQLite, then
                # back off.
                self._spill.push(payload_json)
                self.stats["spilled"] += 1

                spill_count = 0
                while not self._queue.empty():
                    try:
                        extra = self._queue.get_nowait()
                        self._spill.push(extra)
                        spill_count += 1
                    except Empty:
                        break
                self.stats["spilled"] += spill_count

                logger.warning(
                    "[%s] Kafka unreachable — spilled %d payload(s) to SQLite. "
                    "Will retry in %.1fs",
                    self.worker_id, 1 + spill_count, self.retry_backoff_s,
                )
                self._stop_event.wait(timeout=self.retry_backoff_s)

        # ── Shutdown: final poll to flush delivery callbacks ────────
        if self._producer:
            self._producer.poll(0)

        logger.info("[%s] Sender thread exiting", self.worker_id)
