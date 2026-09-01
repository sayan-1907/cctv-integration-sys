"""
stream_worker.py — Layer 1: one isolated OS process per camera.

Design invariants (do not relax these without re-reading the reasoning
in the README):
  1. This process NEVER blocks indefinitely on I/O without a timeout.
  2. The output queue is ALWAYS bounded. On overflow we drop the
     OLDEST frame, never block the capture loop.
  3. A crash here takes down exactly one camera, not the host process.
  4. We decode at target_fps, not the camera's native fps — we throttle
     at the source via OpenCV's CAP_PROP grab/retrieve pattern.
"""

import time
import random
import logging
import multiprocessing as mp
from dataclasses import dataclass
from typing import Optional

import os
os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"
import cv2
import numpy as np

logger = logging.getLogger("layer1.stream_worker")


@dataclass
class ReconnectPolicy:
    initial_delay: float = 2.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: float = 0.3

    def delay_for_attempt(self, attempt: int) -> float:
        base = min(self.initial_delay * (self.multiplier ** attempt), self.max_delay)
        jitter_amount = base * self.jitter
        return base + random.uniform(-jitter_amount, jitter_amount)


class FrameEnvelope:
    """
    What crosses the process boundary to the orchestrator / Layer 2.
    Deliberately NOT the raw high-res frame at full rate — this is
    already the throttled, motion-relevant sample.
    """

    __slots__ = ("camera_id", "timestamp", "frame", "seq")

    def __init__(self, camera_id: str, timestamp: float, frame: np.ndarray, seq: int):
        self.camera_id = camera_id
        self.timestamp = timestamp
        self.frame = frame
        self.seq = seq


def _drop_oldest_put(q: mp.Queue, item, camera_id: str):
    """
    Never block on a full queue. If full, drop the oldest item to make
    room. This is the core memory-safety guarantee for this layer:
    a slow consumer (Layer 2) can NEVER cause unbounded growth here.
    """
    try:
        q.put_nowait(item)
    except Exception:
        try:
            q.get_nowait()  # discard oldest
            logger.debug("Queue full for %s — dropped oldest frame", camera_id)
        except Exception:
            pass
        try:
            q.put_nowait(item)
        except Exception:
            logger.warning("Queue still full for %s — dropped incoming frame", camera_id)


def _run_mock_loop(camera_id, mock_url, frame_interval, out_queue, heartbeat_dict, shutdown_event):
    """
    Generates synthetic frames (moving pattern + timestamp burned in)
    so the whole Layer 1 pipeline — throttling, bounded queues,
    watchdog, reconnect — can be demoed and tested without real
    camera hardware. Occasionally simulates a stream drop so the
    reconnect/backoff path is exercised too.

    mock_url format: mock://<width>x<height>?fail_after=<seconds>
    Optional &image=<path> serves a real static image every frame
    instead of the synthetic pattern — used for end-to-end Layer 2
    testing where a genuine detectable vehicle is needed, not just
    pipeline plumbing.
    """
    import re
    m = re.match(r"mock://(\d+)x(\d+)(?:\?(.*))?", mock_url)
    w, h = (int(m.group(1)), int(m.group(2))) if m else (320, 240)
    params = dict(p.split("=") for p in (m.group(3) or "").split("&") if "=" in p) if m else {}
    fail_after = int(params.get("fail_after", random.randint(20, 45)))
    static_image_path = params.get("image")

    static_frame = None
    if static_image_path:
        static_frame = cv2.imread(static_image_path)
        if static_frame is None:
            logger.warning("Could not load mock image %s — falling back to synthetic pattern",
                            static_image_path)

    seq = 0
    start = time.time()
    heartbeat_dict[camera_id] = {"status": "connected", "last_frame_ts": time.time()}

    while not shutdown_event.is_set():
        now = time.time()
        elapsed = now - start
        if elapsed > fail_after:
            return  # simulate a dropped connection -> triggers reconnect path

        if static_frame is not None:
            frame = static_frame.copy()
            cv2.putText(frame, f"{camera_id} {elapsed:5.1f}s", (10, 30),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)
        else:
            frame = np.zeros((h, w, 3), dtype=np.uint8)
            # Simple moving bar so successive frames are visibly distinct
            bar_x = int((elapsed * 60) % w)
            frame[:, max(0, bar_x - 5):bar_x, :] = (0, 200, 255)
            cv2.putText(frame, f"{camera_id} {elapsed:5.1f}s", (10, h - 15),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 255), 1)

        seq += 1
        heartbeat_dict[camera_id] = {"status": "streaming", "last_frame_ts": now}
        envelope = FrameEnvelope(camera_id, now, frame, seq)
        _drop_oldest_put(out_queue, envelope, camera_id)

        shutdown_event.wait(timeout=frame_interval)


def run_camera_worker(
    camera_id: str,
    rtsp_url: str,
    target_fps: int,
    queue_max_size: int,
    stall_timeout_s: int,
    reconnect_policy: ReconnectPolicy,
    out_queue: mp.Queue,
    heartbeat_dict,          # multiprocessing.Manager().dict() — shared health state
    shutdown_event: mp.Event,
):
    """
    Entry point run inside its own process (see orchestrator.py).
    Blocking calls here only ever affect THIS camera.
    """
    logging.basicConfig(level=logging.INFO,
                         format=f"%(asctime)s [%(levelname)s] [{camera_id}] %(message)s")

    attempt = 0
    seq = 0
    frame_interval = 1.0 / max(target_fps, 1)
    is_mock = str(rtsp_url).startswith("mock://")

    while not shutdown_event.is_set():
        cap = None
        try:
            heartbeat_dict[camera_id] = {"status": "connecting", "last_frame_ts": time.time()}
            logger.info("Connecting to %s", rtsp_url)

            if is_mock:
                # Demo/dev mode: no real camera hardware available.
                # Simulates the exact failure surface (drops, restarts)
                # without needing 80,000 actual RTSP feeds on a laptop.
                _run_mock_loop(camera_id, rtsp_url, frame_interval, out_queue,
                               heartbeat_dict, shutdown_event)
                # _run_mock_loop only returns on shutdown or simulated failure
                if shutdown_event.is_set():
                    break
                raise ConnectionError("Simulated mock stream drop")

            cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
            # Keep OpenCV's internal buffer at 1 — we do our own throttling
            # and don't want a second, uncontrolled buffer inside OpenCV.
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

            if not cap.isOpened():
                raise ConnectionError("VideoCapture failed to open stream")

            attempt = 0  # reset backoff on successful connect
            heartbeat_dict[camera_id] = {"status": "connected", "last_frame_ts": time.time()}
            last_emit = 0.0

            while not shutdown_event.is_set():
                grabbed = cap.grab()  # cheap — just advances the decoder position
                if not grabbed:
                    raise ConnectionError("Stream stopped delivering frames")

                now = time.time()
                if now - last_emit < frame_interval:
                    continue  # throttle: skip decode entirely for dropped frames

                ok, frame = cap.retrieve()  # only decode frames we're actually keeping
                if not ok or frame is None:
                    raise ConnectionError("Frame retrieve failed")

                last_emit = now
                seq += 1
                heartbeat_dict[camera_id] = {"status": "streaming", "last_frame_ts": now}

                envelope = FrameEnvelope(camera_id, now, frame, seq)
                _drop_oldest_put(out_queue, envelope, camera_id)

        except Exception as exc:
            logger.warning("Stream error: %s", exc)
            heartbeat_dict[camera_id] = {"status": "error", "last_frame_ts": time.time(),
                                          "error": str(exc)}
        finally:
            if cap is not None:
                cap.release()

        if shutdown_event.is_set():
            break

        delay = reconnect_policy.delay_for_attempt(attempt)
        attempt += 1
        logger.info("Reconnecting in %.1fs (attempt %d)", delay, attempt)
        heartbeat_dict[camera_id] = {"status": "reconnecting", "last_frame_ts": time.time(),
                                      "next_attempt_in": delay}
        shutdown_event.wait(timeout=delay)  # interruptible sleep

    heartbeat_dict[camera_id] = {"status": "stopped", "last_frame_ts": time.time()}
    logger.info("Worker shutting down cleanly")
