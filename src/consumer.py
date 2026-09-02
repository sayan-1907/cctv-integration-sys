"""
consumer.py — Layer 4: Kafka-to-SQLite Consumer

Consumes structured JSON payloads from the `traffic-anpr-alerts` and
`camera-heartbeats` Kafka topics and persists them into a local SQLite
database. This is the "cloud" half of the hybrid edge-cloud bridge,
adapted for local development without Docker / PostGIS.

Design invariants:
  1. IDEMPOTENT inserts. Each ANPR alert is keyed by
     (camera_id, plate_number, timestamp). Duplicate Kafka messages
     (from at-least-once delivery) are silently ignored via
     INSERT OR IGNORE.
  2. UPSERT for camera status. Heartbeat messages update the
     `camera_registry` row's `status` and `last_seen` columns.
     If the camera doesn't exist yet, it's auto-registered.
  3. Schema auto-creation. Tables and indexes are created on first
     run — no manual migration step needed for the hackathon demo.
  4. Graceful shutdown on SIGINT/SIGTERM. Offsets are committed
     before exit so consumption resumes cleanly.

Usage:
    python consumer.py                          # uses defaults
    python consumer.py --broker localhost:9092   # override broker
"""

import json
import time
import signal
import sqlite3
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger("layer4.consumer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [consumer] %(message)s",
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Database setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCHEMA_SQL = """
-- Camera registry: one row per physical camera, upserted by heartbeats.
CREATE TABLE IF NOT EXISTS camera_registry (
    camera_id       TEXT PRIMARY KEY,
    department_id   TEXT,
    camera_name     TEXT,
    latitude        REAL,
    longitude       REAL,
    status          TEXT DEFAULT 'unknown',
    last_seen       TEXT,
    registered_at   TEXT DEFAULT (datetime('now'))
);

-- ANPR alerts: one row per (camera, plate, timestamp) detection event.
-- The UNIQUE constraint enables idempotent inserts from at-least-once
-- Kafka delivery.
CREATE TABLE IF NOT EXISTS anpr_alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    camera_id       TEXT NOT NULL REFERENCES camera_registry(camera_id)
                    ON DELETE SET NULL,
    plate_number    TEXT NOT NULL,
    confidence      REAL,
    snapshot_path   TEXT,
    detected_at     TEXT NOT NULL,
    ingested_at     TEXT DEFAULT (datetime('now')),
    vehicle_type    TEXT,
    vehicle_color   TEXT,
    UNIQUE (camera_id, plate_number, detected_at)
);

-- Index for the dashboard's "recent alerts" query — most queries
-- will ORDER BY detected_at DESC LIMIT N.
CREATE INDEX IF NOT EXISTS idx_anpr_detected_at
    ON anpr_alerts (detected_at DESC);

-- Index for plate lookups (search-by-plate feature).
CREATE INDEX IF NOT EXISTS idx_anpr_plate
    ON anpr_alerts (plate_number);
"""


def connect_db(db_path: str):
    """
    Connect to the local SQLite database. Creates the file if it
    doesn't exist.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")      # better concurrency
    conn.execute("PRAGMA foreign_keys=ON")        # enforce FK constraints
    logger.info("Connected to SQLite database: %s", db_path)
    return conn


def init_schema(conn):
    """Create tables and indexes if they don't exist, and migrate older schemas."""
    conn.executescript(SCHEMA_SQL)
    
    # Safe migration for new vehicle attributes
    try:
        conn.execute("ALTER TABLE anpr_alerts ADD COLUMN vehicle_type TEXT")
        conn.execute("ALTER TABLE anpr_alerts ADD COLUMN vehicle_color TEXT")
    except sqlite3.OperationalError:
        # Columns likely already exist
        pass
        
    conn.commit()
    logger.info("Database schema initialized")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Payload handlers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _ensure_camera_exists(conn, camera_id: str):
    """
    Make sure a row exists in camera_registry for this camera_id.
    If the camera was never registered via a heartbeat, insert a
    skeleton row so the ANPR alert's FK doesn't fail.
    """
    conn.execute(
        """
        INSERT OR IGNORE INTO camera_registry (camera_id, status, last_seen)
        VALUES (?, 'unknown', datetime('now'))
        """,
        (camera_id,),
    )
    conn.commit()


def handle_anpr_alert(conn, payload: dict):
    """
    Insert an ANPR alert. Idempotent — duplicates are silently ignored.

    Expected payload shape (from ai_worker.build_payload):
        {
            "camera_id": "GJ-AHM-TRF-0001",
            "timestamp": 1693412345.678,
            "plate_number": "GJ01AB1234",
            "confidence_score": 0.87,
            "snapshot_filepath": "../snapshots/GJ-AHM-TRF-0001/1693412345678_GJ01AB1234.jpg"
        }
    """
    camera_id = payload.get("camera_id")
    if not camera_id:
        logger.warning("ANPR alert missing camera_id — skipping: %s", payload)
        return

    _ensure_camera_exists(conn, camera_id)

    import random
    
    # Mock vehicle type and color if not provided by the edge layer
    vehicle_type = payload.get("vehicle_type", random.choice(["Sedan", "SUV", "Two-Wheeler", "Truck", "Hatchback"]))
    vehicle_color = payload.get("vehicle_color", random.choice(["White", "Black", "Silver", "Red", "Blue", "Grey"]))

    detected_at = datetime.fromtimestamp(
        payload["timestamp"], tz=timezone.utc
    ).isoformat()

    conn.execute(
        """
        INSERT OR IGNORE INTO anpr_alerts
            (camera_id, plate_number, confidence, snapshot_path, detected_at, vehicle_type, vehicle_color)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            camera_id,
            payload.get("plate_number", "UNKNOWN"),
            payload.get("confidence_score"),
            payload.get("snapshot_filepath"),
            detected_at,
            vehicle_type,
            vehicle_color,
        ),
    )
    conn.commit()


def handle_heartbeat(conn, payload: dict):
    """
    Upsert camera status from a heartbeat message.

    Expected payload shape:
        {
            "camera_id": "GJ-AHM-TRF-0001",
            "department_id": "GJ-AHM-TRAFFIC-01",
            "name": "Ashram Road - Income Tax Circle",
            "status": "streaming",
            "latitude": 23.0225,
            "longitude": 72.5714,
            "timestamp": 1693412345.678
        }
    """
    camera_id = payload.get("camera_id")
    if not camera_id:
        logger.warning("Heartbeat missing camera_id — skipping: %s", payload)
        return

    lat = payload.get("latitude")
    lon = payload.get("longitude")

    conn.execute(
        """
        INSERT INTO camera_registry
            (camera_id, department_id, camera_name, latitude, longitude,
             status, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(camera_id) DO UPDATE SET
            department_id = COALESCE(excluded.department_id, camera_registry.department_id),
            camera_name   = COALESCE(excluded.camera_name, camera_registry.camera_name),
            latitude      = COALESCE(excluded.latitude, camera_registry.latitude),
            longitude     = COALESCE(excluded.longitude, camera_registry.longitude),
            status        = excluded.status,
            last_seen     = datetime('now')
        """,
        (
            camera_id,
            payload.get("department_id"),
            payload.get("name"),
            lat,
            lon,
            payload.get("status", "unknown"),
        ),
    )
    conn.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Kafka consumer loop
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_consumer(
    broker: str,
    group_id: str,
    topics: list,
    db_path: str,
):
    """
    Main consumer loop. Subscribes to Kafka topics, deserializes JSON
    payloads, routes them to the appropriate handler, and commits
    offsets periodically.
    """
    conn = connect_db(db_path)
    init_schema(conn)

    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "client.id": f"sentinel-consumer-{group_id}",
    })

    consumer.subscribe(topics)
    logger.info(
        "Subscribed to topics %s (broker=%s, group=%s)",
        topics, broker, group_id,
    )

    # Topic → handler dispatch
    handlers = {
        "traffic-anpr-alerts": handle_anpr_alert,
        "camera-heartbeats": handle_heartbeat,
    }

    shutdown = False

    def _signal_handler(signum, frame):
        nonlocal shutdown
        logger.info("Shutdown signal received — committing offsets and exiting")
        shutdown = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    stats = {"processed": 0, "errors": 0, "skipped": 0}

    try:
        while not shutdown:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # normal — we've caught up
                logger.error("Kafka error: %s", msg.error())
                stats["errors"] += 1
                continue

            # Deserialize
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Bad payload on %s: %s", msg.topic(), exc)
                stats["skipped"] += 1
                continue

            # Dispatch to handler
            handler = handlers.get(msg.topic())
            if handler:
                try:
                    handler(conn, payload)
                    stats["processed"] += 1
                except Exception as exc:
                    logger.error(
                        "Handler error on %s: %s (payload=%s)",
                        msg.topic(), exc, payload,
                    )
                    stats["errors"] += 1
            else:
                logger.debug("No handler for topic %s — skipping", msg.topic())
                stats["skipped"] += 1

            # Periodic status log
            if stats["processed"] % 100 == 0 and stats["processed"] > 0:
                logger.info("Consumer stats: %s", stats)

    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer stopped. Final stats: %s", stats)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DEFAULT_DB = str(Path(__file__).resolve().parent.parent / "sentinel.db")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Layer 4 — Kafka-to-SQLite consumer"
    )
    parser.add_argument(
        "--broker", default="localhost:9092",
        help="Kafka broker address (default: localhost:9092)",
    )
    parser.add_argument(
        "--group-id", default="sentinel-layer4",
        help="Kafka consumer group ID (default: sentinel-layer4)",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB,
        help=f"Path to SQLite database file (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--topics", nargs="+",
        default=["traffic-anpr-alerts", "camera-heartbeats"],
        help="Kafka topics to subscribe to",
    )
    args = parser.parse_args()

    run_consumer(
        broker=args.broker,
        group_id=args.group_id,
        topics=args.topics,
        db_path=args.db_path,
    )
