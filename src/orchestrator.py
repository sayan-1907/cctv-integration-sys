"""
orchestrator.py — Layer 1 entry point.

Responsibilities:
  - Load department config
  - Enforce max_concurrent_streams (refuse to overload legacy hardware)
  - Spawn one isolated process per camera (stream_worker.run_camera_worker)
  - Run a watchdog that force-restarts stalled workers (connected but
    silently frozen — different failure mode from "disconnected")
  - Spawn Layer 2 AI worker processes and hand them direct references
    to Layer 1's per-camera queues (same-host, in-memory — frames
    never touch disk or network to make this hop)
  - Pass Layer 3 (Kafka) configuration to AI workers so each worker
    process can instantiate its own fault-tolerant publisher
  - Emit periodic heartbeat summaries (this is the ONLY thing this
    layer will eventually push toward the center — camera up/down
    status — never video)
"""

import time
import signal
import logging
import threading
import multiprocessing as mp
from pathlib import Path

import yaml

from stream_worker import run_camera_worker, ReconnectPolicy
from ai_worker import run_ai_worker

logger = logging.getLogger("layer1.orchestrator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class EdgeOrchestrator:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.manager = mp.Manager()
        self.heartbeats = self.manager.dict()
        self.ai_heartbeats = self.manager.dict()
        self.shutdown_event = mp.Event()
        self.workers: dict[str, dict] = {}   # camera_id -> {process, queue, cfg}
        self.ai_workers: list[mp.Process] = []
        self._heartbeat_publisher = None
        self._heartbeat_thread = None

    def _load_config(self) -> dict:
        with open(self.config_path, "r") as f:
            return yaml.safe_load(f)

    def _enabled_cameras(self) -> list[dict]:
        cams = [c for c in self.config["cameras"] if c.get("enabled", True)]
        limit = self.config["max_concurrent_streams"]
        if len(cams) > limit:
            logger.warning(
                "Config lists %d enabled cameras but max_concurrent_streams=%d — "
                "refusing to start the remaining %d to protect host resources. "
                "Raise the limit only after confirming host headroom.",
                len(cams), limit, len(cams) - limit,
            )
            cams = cams[:limit]
        return cams

    def start(self):
        reconnect_cfg = self.config.get("reconnect", {})
        policy = ReconnectPolicy(
            initial_delay=reconnect_cfg.get("initial_delay", 2),
            max_delay=reconnect_cfg.get("max_delay", 60),
            multiplier=reconnect_cfg.get("multiplier", 2),
            jitter=reconnect_cfg.get("jitter", 0.3),
        )

        for cam in self._enabled_cameras():
            q = mp.Queue(maxsize=self.config["queue_max_size"])
            p = mp.Process(
                target=run_camera_worker,
                name=f"cam-{cam['id']}",
                kwargs=dict(
                    camera_id=cam["id"],
                    rtsp_url=cam["rtsp_url"],
                    target_fps=self.config["target_decode_fps"],
                    queue_max_size=self.config["queue_max_size"],
                    stall_timeout_s=self.config["stall_timeout_seconds"],
                    reconnect_policy=policy,
                    out_queue=q,
                    heartbeat_dict=self.heartbeats,
                    shutdown_event=self.shutdown_event,
                ),
                daemon=True,
            )
            p.start()
            self.workers[cam["id"]] = {"process": p, "queue": q, "cfg": cam}
            logger.info("Spawned worker for %s (pid=%d)", cam["id"], p.pid)

        logger.info("Started %d camera workers (department=%s, ceiling=%d)",
                     len(self.workers), self.config["department_id"],
                     self.config["max_concurrent_streams"])

        self._start_layer2_workers()
        self._start_heartbeat_publisher()

    def _start_layer2_workers(self):
        """
        Spawns the Layer 2 AI worker pool. Deliberately N workers
        sharing M camera queues rather than one worker per camera —
        see ai_worker.py docstring for why (model memory footprint).
        """
        layer2_cfg = self.config.get("layer2")
        if not layer2_cfg:
            logger.warning("No 'layer2' config block found — skipping AI worker startup. "
                           "Layer 1 will keep running and queues will just fill/drop.")
            return

        camera_queue_map = {cid: info["queue"] for cid, info in self.workers.items()}
        if not camera_queue_map:
            logger.warning("No camera workers running — skipping Layer 2 startup")
            return

        # Layer 3 config is passed through to each AI worker process so
        # it can instantiate its own Kafka publisher independently. If
        # the 'layer3' block is missing, AI workers fall back to stdout-only.
        layer3_cfg = self.config.get("layer3")
        if layer3_cfg:
            logger.info("Layer 3 Kafka config found — AI workers will publish to %s @ %s",
                         layer3_cfg.get("kafka_topic", "traffic-anpr-alerts"),
                         layer3_cfg.get("kafka_broker", "localhost:9092"))
        else:
            logger.info("No 'layer3' config block — AI workers will output to stdout only")

        worker_count = layer2_cfg.get("ai_worker_count", 1)
        for i in range(worker_count):
            self._spawn_ai_worker(f"ai-worker-{i}", camera_queue_map, layer2_cfg, layer3_cfg)

        logger.info("Started %d Layer 2 AI worker(s) sharing %d camera queue(s)",
                     worker_count, len(camera_queue_map))

    def _spawn_ai_worker(self, worker_id: str, camera_queue_map: dict,
                         layer2_cfg: dict, layer3_cfg: dict = None):
        p = mp.Process(
            target=run_ai_worker,
            name=worker_id,
            kwargs=dict(
                worker_id=worker_id,
                camera_queue_map=camera_queue_map,
                config=layer2_cfg,
                shutdown_event=self.shutdown_event,
                ai_heartbeat_dict=self.ai_heartbeats,
                layer3_config=layer3_cfg,
            ),
            daemon=True,
        )
        p.start()
        self.ai_workers.append(p)
        logger.info("Spawned Layer 2 AI worker %s (pid=%d)", worker_id, p.pid)

    def _restart_worker(self, camera_id: str):
        """Force-kill and respawn a stalled worker."""
        info = self.workers[camera_id]
        old_proc: mp.Process = info["process"]
        logger.warning("Force-restarting stalled worker for %s (pid=%d)",
                        camera_id, old_proc.pid)
        old_proc.terminate()
        old_proc.join(timeout=5)
        if old_proc.is_alive():
            old_proc.kill()  # SIGKILL as last resort — never let a wedged process linger

        cam = info["cfg"]
        q = mp.Queue(maxsize=self.config["queue_max_size"])
        policy = ReconnectPolicy(**{k: v for k, v in self.config.get("reconnect", {}).items()})
        p = mp.Process(
            target=run_camera_worker,
            name=f"cam-{camera_id}",
            kwargs=dict(
                camera_id=camera_id,
                rtsp_url=cam["rtsp_url"],
                target_fps=self.config["target_decode_fps"],
                queue_max_size=self.config["queue_max_size"],
                stall_timeout_s=self.config["stall_timeout_seconds"],
                reconnect_policy=policy,
                out_queue=q,
                heartbeat_dict=self.heartbeats,
                shutdown_event=self.shutdown_event,
            ),
            daemon=True,
        )
        p.start()
        self.workers[camera_id] = {"process": p, "queue": q, "cfg": cam}

    def watchdog_tick(self):
        """
        Call this periodically from the main loop. Detects two distinct
        failure modes:
          - dead process (crashed outright) -> respawn
          - alive process, but no frame for > stall_timeout -> force restart
            (this catches frozen/wedged streams that a plain liveness
            check on the process would miss entirely)
        """
        stall_timeout = self.config["stall_timeout_seconds"]
        now = time.time()

        for camera_id, info in list(self.workers.items()):
            proc: mp.Process = info["process"]

            if not proc.is_alive():
                logger.error("Worker for %s died (exitcode=%s) — respawning",
                             camera_id, proc.exitcode)
                self._restart_worker(camera_id)
                continue

            hb = self.heartbeats.get(camera_id)
            if hb and hb["status"] in ("connected", "streaming"):
                if now - hb["last_frame_ts"] > stall_timeout:
                    self._restart_worker(camera_id)

        self._watchdog_ai_workers()

    def _watchdog_ai_workers(self):
        """
        AI workers are stateless with respect to any single frame (if
        one dies mid-inference, the frame is simply lost — same as any
        dropped frame elsewhere in this pipeline), so recovery is just
        'notice it died, respawn it'. No stall detection needed here
        the way Layer 1 needs it: there's no persistent connection to
        freeze, just a queue-polling loop.
        """
        layer2_cfg = self.config.get("layer2")
        if not layer2_cfg:
            return

        layer3_cfg = self.config.get("layer3")
        camera_queue_map = {cid: info["queue"] for cid, info in self.workers.items()}

        for i, proc in enumerate(list(self.ai_workers)):
            if not proc.is_alive():
                worker_id = proc.name
                logger.error("Layer 2 worker %s died (exitcode=%s) — respawning",
                             worker_id, proc.exitcode)
                self.ai_workers.remove(proc)
                self._spawn_ai_worker(worker_id, camera_queue_map, layer2_cfg, layer3_cfg)

    def status_summary(self) -> dict:
        """
        This is the ONLY payload this layer sends upward — camera
        liveness, never video. Downstream this maps directly onto the
        PostGIS camera_registry.status column.
        """
        summary = {}
        for camera_id in self.workers:
            hb = self.heartbeats.get(camera_id, {"status": "unknown"})
            summary[camera_id] = hb.get("status", "unknown")
        return summary

    def ai_status_summary(self) -> dict:
        """Layer 2 worker status — model-loading/ready/processing/stopped/failed."""
        summary = {}
        for proc in self.ai_workers:
            hb = self.ai_heartbeats.get(proc.name, {"status": "unknown"})
            summary[proc.name] = hb.get("status", "unknown")
        return summary

    # ── Heartbeat publisher (edge → cloud camera status) ─────────

    def _start_heartbeat_publisher(self):
        """
        Initialise a FaultTolerantKafkaPublisher for the camera-heartbeats
        topic and spawn a daemon thread that publishes enriched camera
        status every ~20 seconds. If Layer 3 config is absent or
        confluent-kafka is not installed, heartbeat publishing is silently
        disabled — the orchestrator keeps running either way.
        """
        layer3_cfg = self.config.get("layer3")
        if not layer3_cfg:
            logger.info("No 'layer3' config — heartbeat publishing disabled")
            return

        try:
            from kafka_publisher import FaultTolerantKafkaPublisher
        except ImportError:
            logger.warning(
                "kafka_publisher / confluent-kafka not available — "
                "heartbeat publishing disabled"
            )
            return

        try:
            self._heartbeat_publisher = FaultTolerantKafkaPublisher(
                worker_id="heartbeat-publisher",
                broker_url=layer3_cfg.get("kafka_broker", "localhost:9092"),
                topic=layer3_cfg.get("heartbeat_topic", "camera-heartbeats"),
                spill_dir=layer3_cfg.get("kafka_spill_dir", "../kafka_spill"),
                max_in_memory=100,
                retry_backoff_s=layer3_cfg.get("kafka_retry_backoff_s", 5.0),
            )
            self._heartbeat_publisher.start()
        except Exception as exc:
            logger.error("Failed to start heartbeat publisher: %s", exc)
            self._heartbeat_publisher = None
            return

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="heartbeat-publisher",
            daemon=True,
        )
        self._heartbeat_thread.start()
        logger.info("Heartbeat publisher started (topic=%s, interval=20s)",
                     layer3_cfg.get("heartbeat_topic", "camera-heartbeats"))

    def _heartbeat_loop(self, interval_s: float = 20.0):
        """
        Background thread that publishes one heartbeat message per camera
        every `interval_s` seconds. Each message includes the camera's
        config metadata (lat/lon, name, department) so the cloud consumer
        can populate the PostGIS camera_registry without any separate
        registration step.
        """
        department_id = self.config.get("department_id", "unknown")

        # Build a quick lookup from camera_id → config dict so we can
        # enrich the status with location/name without re-scanning the
        # YAML list every tick.
        cam_cfg_map = {}
        for cam in self.config.get("cameras", []):
            cam_cfg_map[cam["id"]] = cam

        while not self.shutdown_event.is_set():
            try:
                statuses = self.status_summary()
                now = time.time()

                for camera_id, status in statuses.items():
                    cfg = cam_cfg_map.get(camera_id, {})
                    payload = {
                        "camera_id":     camera_id,
                        "department_id": department_id,
                        "name":          cfg.get("name", ""),
                        "status":        status,
                        "latitude":      cfg.get("latitude"),
                        "longitude":     cfg.get("longitude"),
                        "timestamp":     now,
                    }
                    self._heartbeat_publisher.publish(payload)

                logger.debug("Published heartbeat for %d camera(s)", len(statuses))
            except Exception as exc:
                logger.warning("Heartbeat publish error: %s", exc)

            # Use shutdown_event.wait() instead of time.sleep() so
            # the thread wakes up immediately on shutdown signal.
            self.shutdown_event.wait(timeout=interval_s)

        logger.info("Heartbeat thread exiting")

    def _stop_heartbeat_publisher(self):
        """Gracefully stop the heartbeat publisher and join the thread."""
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            # shutdown_event is already set by run_forever's signal handler,
            # so the thread will exit on its next loop iteration.
            self._heartbeat_thread.join(timeout=5)
        if self._heartbeat_publisher:
            self._heartbeat_publisher.stop()
            logger.info("Heartbeat publisher stopped")

    def run_forever(self, watchdog_interval_s: float = 5.0):
        self.start()

        def _handle_sigterm(signum, frame):
            logger.info("Shutdown signal received — stopping all camera workers")
            self.shutdown_event.set()

        signal.signal(signal.SIGTERM, _handle_sigterm)
        signal.signal(signal.SIGINT, _handle_sigterm)

        try:
            while not self.shutdown_event.is_set():
                self.watchdog_tick()
                logger.info("Layer1 Status: %s", self.status_summary())
                logger.info("Layer2 Status: %s", self.ai_status_summary())
                time.sleep(watchdog_interval_s)
        finally:
            self._stop_heartbeat_publisher()
            for info in self.workers.values():
                info["process"].join(timeout=5)
            for proc in self.ai_workers:
                proc.join(timeout=5)
            logger.info("All workers stopped. Clean exit.")


if __name__ == "__main__":
    import sys
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config/cameras.yaml"
    orchestrator = EdgeOrchestrator(config_file)
    orchestrator.run_forever()
