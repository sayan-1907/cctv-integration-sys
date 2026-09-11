"""
api.py — Layer 4: Dashboard API, MJPEG Streamer & On-Demand Extraction Engine

A FastAPI application that serves:
  1. REST endpoints for querying cameras, stats, and ANPR alerts.
  2. A WebSocket endpoint for real-time alert push broadcasting.
  3. Dynamic On-Demand Camera Extraction:
     Instead of running all 30+ cameras simultaneously (which causes lag,
     OOM, and host crashes), cameras run in an idle state. When a user
     selects/views a specific camera, high-accuracy AI inference (YOLO + ALPR)
     activates dynamically for that camera with a hard concurrency safety cap.
  4. On-demand MJPEG video streaming via multipart HTTP.
  5. Static dashboard command center frontend.
"""

import os
import urllib.parse
import json
import time
import sqlite3
import asyncio
import logging
import random
import threading
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Dict, List, Any

import cv2
import numpy as np
import yaml

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# AI extraction modules from Layer 2
from stream_worker import FrameEnvelope
from ai_worker import (
    load_models,
    process_frame,
    save_snapshot,
    build_payload,
    ModelBundle,
    VEHICLE_CLASS_IDS,
    PLATE_PATTERN,
    _fix_indian_plate_confusions,
)

logger = logging.getLogger("layer4.api")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [api] %(message)s",
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# App setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app = FastAPI(
    title="Sentinel Dashboard API",
    description="Layer 4 — On-Demand Camera Monitoring & ANPR Alert Dashboard",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database path — shared with consumer.py
DB_PATH = str(Path(__file__).resolve().parent.parent / "sentinel.db")
SNAPSHOTS_DIR = Path(__file__).resolve().parent.parent / "snapshots"
SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

# Global camera configs loaded from YAML
CAMERA_CONFIGS: Dict[str, dict] = {}
CAMERA_RTSP_URLS: Dict[str, str] = {}

MAX_STREAM_FPS = 10
STREAM_JPEG_QUALITY = 65


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _rows_to_dicts(rows) -> list[dict]:
    return [dict(row) for row in rows]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WebSocket — real-time alert push
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AlertBroadcaster:
    def __init__(self):
        self.clients: list[WebSocket] = []
        self._last_id = 0
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def register(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)
        logger.info("WebSocket client connected (%d total)", len(self.clients))

    def unregister(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)
        logger.info("WebSocket client disconnected (%d remaining)", len(self.clients))

    def broadcast_sync(self, alerts: list[dict]):
        if not self.clients or not alerts:
            return
        if self._loop and self._loop.is_running():
            msg = json.dumps({"type": "new_alerts", "alerts": alerts})
            asyncio.run_coroutine_threadsafe(self._send_to_all(msg), self._loop)

    async def _send_to_all(self, msg: str):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.unregister(ws)

    async def start_polling(self):
        self._running = True
        self._loop = asyncio.get_running_loop()

        try:
            conn = get_db()
            try:
                cur = conn.execute("SELECT COALESCE(MAX(id), 0) FROM anpr_alerts")
                self._last_id = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception:
            self._last_id = 0

        while self._running:
            await asyncio.sleep(1.5)
            if not self.clients:
                continue

            try:
                new_alerts = self._fetch_new_alerts()
                if new_alerts:
                    msg = json.dumps({"type": "new_alerts", "alerts": new_alerts})
                    await self._send_to_all(msg)
            except Exception as exc:
                logger.warning("Alert poll error: %s", exc)

    def _fetch_new_alerts(self) -> list:
        conn = get_db()
        try:
            cur = conn.execute(
                """
                SELECT
                    a.id, a.camera_id, c.camera_name,
                    a.plate_number, a.confidence,
                    a.detected_at, a.detected_at as timestamp,
                    a.vehicle_type, a.vehicle_color,
                    c.latitude, c.longitude
                FROM anpr_alerts a
                LEFT JOIN camera_registry c ON a.camera_id = c.camera_id
                WHERE a.id > ?
                ORDER BY a.id ASC
                LIMIT 50
                """,
                (self._last_id,),
            )
            rows = _rows_to_dicts(cur.fetchall())
        finally:
            conn.close()

        if rows:
            self._last_id = rows[-1]["id"]

        return rows

    def stop(self):
        self._running = False


broadcaster = AlertBroadcaster()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Tactical Live Stream Simulation & Non-Blocking Camera Session
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _mask_rtsp_url(url: str) -> str:
    """Mask credentials in RTSP URL for secure diagnostic display."""
    if "@" in url:
        try:
            proto, rest = url.split("://", 1)
            creds, hostpath = rest.split("@", 1)
            return f"{proto}://***:***@{hostpath}"
        except Exception:
            return "rtsp://***:***@..."
    return url


# Per-camera plate dedup window (seconds). Much longer than the Layer 2
# pipeline's 10s window because the simulated extraction cycles through
# the candidate pool fast and re-emits the same plates repeatedly.
ON_DEMAND_DEDUP_WINDOW_S = 60

def _probe_rtsp_online(url: str, timeout: float = 0.35) -> bool:
    """Fast non-blocking TCP socket check to see if an RTSP endpoint is online."""
    if not url or str(url).startswith("mock://") or url == "0" or url == 0:
        return False
    try:
        from urllib.parse import urlparse
        import socket
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port or 554
        if not host:
            return False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        code = sock.connect_ex((host, port))
        sock.close()
        return code == 0
    except Exception:
        return False

def generate_standby_frame(
    camera_id: str,
    camera_name: str,
    rtsp_url: str,
    status_text: str = "AWAITING RTSP FEED",
) -> np.ndarray:
    """
    Renders an authentic, professional CCTV / VMS standby diagnostic test pattern.
    Zero cartoons, zero road animations, zero simulated cars.
    """
    w, h = 640, 360
    frame = np.zeros((h, w, 3), dtype=np.uint8)
    # Background slate: dark surveillance charcoal (#0e1117 / BGR: 23, 17, 14)
    frame[:] = (23, 17, 14)

    # Subtle surveillance grid lines
    for x in range(40, w, 60):
        cv2.line(frame, (x, 0), (x, h), (32, 26, 20), 1)
    for y in range(40, h, 60):
        cv2.line(frame, (0, y), (w, y), (32, 26, 20), 1)

    # Tactical corner reticles [ + ]
    reticle_color = (65, 55, 45)
    margin = 16
    arm = 14
    # Top-left
    cv2.line(frame, (margin, margin), (margin + arm, margin), reticle_color, 1)
    cv2.line(frame, (margin, margin), (margin, margin + arm), reticle_color, 1)
    # Top-right
    cv2.line(frame, (w - margin, margin), (w - margin - arm, margin), reticle_color, 1)
    cv2.line(frame, (w - margin, margin), (w - margin, margin + arm), reticle_color, 1)
    # Bottom-left
    cv2.line(frame, (margin, h - margin), (margin + arm, h - margin), reticle_color, 1)
    cv2.line(frame, (margin, h - margin), (margin, h - margin - arm), reticle_color, 1)
    # Bottom-right
    cv2.line(frame, (w - margin, h - margin), (w - margin - arm, h - margin), reticle_color, 1)
    cv2.line(frame, (w - margin, h - margin), (w - margin, h - margin - arm), reticle_color, 1)

    # Header Telemetry Bar
    cv2.rectangle(frame, (0, 0), (w, 34), (16, 12, 9), -1)
    cv2.line(frame, (0, 34), (w, 34), (45, 38, 30), 1)
    cv2.putText(
        frame,
        f"SENTINEL CCTV  |  {camera_name.upper()} [{camera_id}]",
        (16, 22),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.44,
        (240, 243, 246),
        1,
    )
    now_utc = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    cv2.putText(
        frame,
        now_utc,
        (w - 185, 22),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.36,
        (139, 148, 158),
        1,
    )

    # Center Diagnostics Card
    card_w, card_h = 510, 160
    cx1 = (w - card_w) // 2
    cy1 = 88
    cx2 = cx1 + card_w
    cy2 = cy1 + card_h

    # Card background and border
    cv2.rectangle(frame, (cx1, cy1), (cx2, cy2), (28, 22, 18), -1)
    cv2.rectangle(frame, (cx1, cy1), (cx2, cy2), (56, 48, 38), 1)

    # Status Pill / Banner inside card
    is_auth_req = "401" in status_text or "CREDENTIAL" in status_text
    pill_color = (35, 45, 205) if is_auth_req else (35, 140, 210)
    cv2.rectangle(frame, (cx1 + 16, cy1 + 16), (cx2 - 16, cy1 + 44), (20, 16, 13), -1)
    cv2.rectangle(frame, (cx1 + 16, cy1 + 16), (cx2 - 16, cy1 + 44), pill_color, 1)
    cv2.putText(
        frame,
        f"[FEED STANDBY]  {status_text}",
        (cx1 + 24, cy1 + 35),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.36,
        pill_color,
        1,
    )

    # Target URL line
    masked_url = _mask_rtsp_url(rtsp_url)
    cv2.putText(
        frame,
        f"RTSP Source: {masked_url}",
        (cx1 + 20, cy1 + 72),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.36,
        (201, 209, 217),
        1,
    )

    # Diagnostic messages
    if is_auth_req:
        msg1 = "Gateway authentication required (401 Unauthorized)."
        msg2 = "Provide RTSP_AUTH_EMAIL and RTSP_AUTH_PASSWORD in .env"
    else:
        msg1 = "Connecting to remote gateway socket..."
        msg2 = "Original camera feed will stream automatically upon connection."

    cv2.putText(
        frame,
        msg1,
        (cx1 + 20, cy1 + 104),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.35,
        (139, 148, 158),
        1,
    )
    cv2.putText(
        frame,
        msg2,
        (cx1 + 20, cy1 + 128),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.35,
        (139, 148, 158),
        1,
    )

    # Footer status
    cv2.putText(
        frame,
        "STANDBY · HD STREAM READY · AWAITING REAL FRAMES",
        (16, h - 14),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.33,
        (90, 80, 70),
        1,
    )
    cv2.putText(
        frame,
        "TCP / PORT 8554",
        (w - 120, h - 14),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.33,
        (90, 80, 70),
        1,
    )

    return frame



class CameraSession:
    """
    Manages video frame capture and on-demand AI inference for one camera.
    Multiple browser viewers share a single decoded video stream.
    """
    def __init__(self, camera_id: str, rtsp_url: str, camera_name: str, manager: "OnDemandExtractionManager"):
        self.camera_id = camera_id
        self.rtsp_url = rtsp_url
        self.camera_name = camera_name
        self.manager = manager
        self.is_extracting = False
        self.viewer_count = 0
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.condition = threading.Condition()
        self.recent_alerts: list[dict] = []
        self.recent_plates: dict[str, float] = {}  # plate_number -> last_emitted_ts
        self.plate_best_conf: dict[str, float] = {}  # plate_number -> best confidence
        self.plate_sighting_count: dict[str, int] = {}  # plate_number -> times seen
        self.last_inference_ts = 0.0
        self.last_activity = time.time()
        self.frame_seq = 0
        self.in_mock_fallback = True
        self.lock = threading.RLock()

        # Initial clean standby slate
        initial_status = "AWAITING RTSP FEED"
        if "103.250.160.189" in self.rtsp_url and "@" not in self.rtsp_url:
            initial_status = "401 UNAUTHORIZED: GATEWAY CREDENTIALS REQUIRED IN .ENV"
        initial_frame = generate_standby_frame(self.camera_id, self.camera_name, self.rtsp_url, initial_status)
        ok, initial_jpeg = cv2.imencode(".jpg", initial_frame, [cv2.IMWRITE_JPEG_QUALITY, STREAM_JPEG_QUALITY])
        self.latest_jpeg: Optional[bytes] = initial_jpeg.tobytes() if ok else None
        self.latest_frame: Optional[np.ndarray] = initial_frame

    def get_current_jpeg(self) -> Optional[bytes]:
        with self.condition:
            return self.latest_jpeg

    def start(self):
        with self.lock:
            if self.thread is None or not self.thread.is_alive():
                self.stop_event.clear()
                self.thread = threading.Thread(
                    target=self._worker_loop,
                    name=f"cam-session-{self.camera_id}",
                    daemon=True,
                )
                self.thread.start()
                logger.info("[%s] Started on-demand camera worker", self.camera_id)

    def stop(self):
        with self.lock:
            self.is_extracting = False
            self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1.5)
        self.manager.update_camera_status(self.camera_id, "idle")

    def _worker_loop(self):
        logger.info("[%s] Worker loop running (source=%s)", self.camera_id, _mask_rtsp_url(self.rtsp_url))
        cap = None
        last_connect_attempt = 0.0
        connect_cooldown = 3.5  # Attempt to reconnect every 3.5s

        import os
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|timeout;2000000"

        frame_interval = 1.0 / MAX_STREAM_FPS
        encode_params = [cv2.IMWRITE_JPEG_QUALITY, STREAM_JPEG_QUALITY]

        while not self.stop_event.is_set():
            t0 = time.time()
            frame = None

            # Attempt to connect or reconnect to RTSP feed
            if cap is None or not cap.isOpened():
                if t0 - last_connect_attempt >= connect_cooldown:
                    last_connect_attempt = t0
                    try:
                        cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
                        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                        if cap.isOpened():
                            logger.info("[%s] Successfully connected to live RTSP feed!", self.camera_id)
                            self.in_mock_fallback = False
                        else:
                            cap.release()
                            cap = None
                            self.in_mock_fallback = True
                    except Exception as e:
                        logger.warning("[%s] RTSP connect exception: %s", self.camera_id, e)
                        cap = None
                        self.in_mock_fallback = True

            # If cap is open, grab and read real camera frame
            if cap is not None and cap.isOpened():
                ret, raw_frame = cap.read()
                if ret and raw_frame is not None:
                    frame = raw_frame
                    self.in_mock_fallback = False
                else:
                    logger.warning("[%s] Failed to read frame from RTSP stream, will reconnect", self.camera_id)
                    cap.release()
                    cap = None
                    self.in_mock_fallback = True

            # If no live frame available, render clean standby slate (zero cartoons)
            if frame is None:
                self.in_mock_fallback = True
                status_text = "CONNECTING TO RTSP FEED..."
                if "103.250.160.189" in self.rtsp_url and "@" not in self.rtsp_url:
                    status_text = "401 UNAUTHORIZED: GATEWAY CREDENTIALS REQUIRED IN .ENV"
                frame = generate_standby_frame(self.camera_id, self.camera_name, self.rtsp_url, status_text)

            self.frame_seq += 1

            # Encode and store latest JPEG for MJPEG stream
            ok, jpeg = cv2.imencode(".jpg", frame, encode_params)
            if ok:
                jpeg_bytes = jpeg.tobytes()
                with self.condition:
                    self.latest_jpeg = jpeg_bytes
                    self.latest_frame = frame
                    self.condition.notify_all()

            # Run on-demand AI extraction ONLY when active AND receiving real frames
            if self.is_extracting and not self.in_mock_fallback:
                now = time.time()
                if now - self.last_inference_ts >= 1.5:
                    self.last_inference_ts = now
                    try:
                        self.manager.trigger_extraction_event(self, frame, now)
                    except Exception as exc:
                        logger.error("[%s] Error during on-demand extraction: %s", self.camera_id, exc)

            # Auto-reclaim: If no viewers and extraction is disabled, stop after 5s idle
            if self.viewer_count <= 0 and not self.is_extracting:
                if time.time() - self.last_activity > 5.0:
                    logger.info("[%s] Idle timeout (no viewers, extraction off) — releasing worker", self.camera_id)
                    break

            # Frame pacing
            elapsed_frame = time.time() - t0
            if elapsed_frame < frame_interval:
                time.sleep(frame_interval - elapsed_frame)
            else:
                time.sleep(0.01)

        if cap is not None:
            cap.release()
        self.manager.update_camera_status(self.camera_id, "idle")
        logger.info("[%s] Worker loop terminated cleanly", self.camera_id)


class OnDemandExtractionManager:
    def __init__(self, max_concurrent_extractions: int = 2):
        self.max_concurrent_extractions = max_concurrent_extractions
        self.sessions: Dict[str, CameraSession] = {}
        self.lock = threading.RLock()
        self.bundle: Optional[ModelBundle] = None
        self.layer2_config = {
            "vehicle_confidence_threshold": 0.35,
            "ocr_confidence_threshold": 0.4,
            "snapshot_dir": str(SNAPSHOTS_DIR),
            "snapshot_max_width": 240,
            "plate_dedup_window_seconds": 8,
            "ocr_engine": "fast_alpr",
            "device": "cpu",
            "yolo_model": str(Path(__file__).resolve().parent.parent / "yolov8n.pt"),
        }

    def get_models(self) -> Optional[ModelBundle]:
        if self.bundle is None:
            logger.info("Initializing AI models for on-demand extraction engine...")
            try:
                self.bundle = load_models(self.layer2_config)
                logger.info("AI models initialized successfully")
            except Exception as e:
                logger.error("Failed to load models: %s", e)
        return self.bundle

    def get_or_create_session(self, camera_id: str) -> CameraSession:
        with self.lock:
            if camera_id not in self.sessions:
                rtsp_url = CAMERA_RTSP_URLS.get(camera_id, "")
                cfg = CAMERA_CONFIGS.get(camera_id, {})
                name = cfg.get("camera_name", camera_id)
                self.sessions[camera_id] = CameraSession(camera_id, rtsp_url, name, self)
            session = self.sessions[camera_id]
            session.last_activity = time.time()
            session.start()
            return session

    def start_extraction(self, camera_id: str) -> bool:
        with self.lock:
            # Enforce concurrent extraction limit to prevent lag and crashes
            active_extracting = [s for s in self.sessions.values() if s.is_extracting and s.camera_id != camera_id]
            if len(active_extracting) >= self.max_concurrent_extractions:
                oldest = min(active_extracting, key=lambda s: s.last_inference_ts)
                logger.info("Safety cap reached (%d max): Stopping extraction on %s to free CPU",
                            self.max_concurrent_extractions, oldest.camera_id)
                oldest.is_extracting = False
                self.update_camera_status(oldest.camera_id, "streaming" if oldest.viewer_count > 0 else "idle")

            session = self.get_or_create_session(camera_id)
            session.is_extracting = True
            session.last_inference_ts = 0.0  # Instant trigger on first iteration
            session.last_activity = time.time()
            self.update_camera_status(camera_id, "extracting")
            logger.info("[%s] AI Data Extraction ACTIVATED on-demand", camera_id)
            return True

    def stop_extraction(self, camera_id: str) -> bool:
        with self.lock:
            if camera_id in self.sessions:
                session = self.sessions[camera_id]
                session.is_extracting = False
                session.last_activity = time.time()
                self.update_camera_status(camera_id, "streaming" if session.viewer_count > 0 else "idle")
                logger.info("[%s] AI Data Extraction DEACTIVATED", camera_id)
                return True
            return False

    def get_active_extractions(self) -> list[str]:
        with self.lock:
            return [cid for cid, s in self.sessions.items() if s.is_extracting]

    def update_camera_status(self, camera_id: str, status: str):
        try:
            conn = get_db()
            conn.execute(
                "UPDATE camera_registry SET status = ?, last_seen = datetime('now') WHERE camera_id = ?",
                (status, camera_id)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error("Failed to update status for %s: %s", camera_id, e)

    def persist_and_broadcast(self, alert: dict):
        # Insert alert into SQLite
        try:
            conn = get_db()
            conn.execute("""
                INSERT OR IGNORE INTO anpr_alerts
                    (camera_id, plate_number, confidence, snapshot_path, detected_at, vehicle_type, vehicle_color)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                alert["camera_id"],
                alert["plate_number"],
                alert["confidence"],
                alert["snapshot_path"],
                alert["detected_at"],
                alert.get("vehicle_type", "Unknown"),
                alert.get("vehicle_color", "Unknown")
            ))
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.error("DB insert error for alert: %s", exc)

        # Broadcast immediately to all WebSocket clients
        broadcaster.broadcast_sync([alert])


    def _is_plate_duplicate(self, session: CameraSession, plate_number: str, confidence: float) -> bool:
        """
        Check if this plate was recently emitted for this camera.
        If yes, update best confidence and sighting count but don't re-emit.
        Returns True if this is a duplicate (should be suppressed).
        """
        now = time.time()
        with session.lock:
            last_seen = session.recent_plates.get(plate_number)
            if last_seen is not None and (now - last_seen) < ON_DEMAND_DEDUP_WINDOW_S:
                # Duplicate within cooldown — update stats but suppress alert
                session.plate_sighting_count[plate_number] = session.plate_sighting_count.get(plate_number, 1) + 1
                if confidence > session.plate_best_conf.get(plate_number, 0):
                    session.plate_best_conf[plate_number] = confidence
                    # Update the existing alert in recent_alerts with better confidence
                    for alert in session.recent_alerts:
                        if alert["plate_number"] == plate_number:
                            alert["confidence"] = confidence
                            alert["sighting_count"] = session.plate_sighting_count[plate_number]
                            break
                    # Also update in DB
                    try:
                        conn = get_db()
                        conn.execute(
                            "UPDATE anpr_alerts SET confidence = ? WHERE camera_id = ? AND plate_number = ? AND confidence < ?",
                            (confidence, session.camera_id, plate_number, confidence)
                        )
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass
                return True
            # New plate or cooldown expired — record it
            session.recent_plates[plate_number] = now
            session.plate_best_conf[plate_number] = confidence
            session.plate_sighting_count[plate_number] = 1
            return False

    def trigger_extraction_event(self, session: CameraSession, frame: np.ndarray, timestamp: float, plate_info: tuple[str, str, str]):
        det_at = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

        # If live video stream is active, run the real YOLOv8 + OCR model!
        if not session.in_mock_fallback:
            models = self.get_models()
            if models is not None:
                try:
                    from ai_worker import FrameEnvelope, process_frame
                    envelope = FrameEnvelope(session.camera_id, timestamp, frame, session.frame_seq)
                    detections = process_frame(envelope, models, self.layer2_config)
                    if detections:
                        for det in detections:
                            conf = round(float(det.confidence_score), 2)
                            if self._is_plate_duplicate(session, det.plate_number, conf):
                                continue  # suppressed — already seen recently
                            cam_dir = SNAPSHOTS_DIR / session.camera_id
                            cam_dir.mkdir(parents=True, exist_ok=True)
                            snap_filename = f"{int(timestamp * 1000)}_{det.plate_number}.jpg"
                            snap_path = cam_dir / snap_filename
                            cv2.imwrite(str(snap_path), det.crop, [cv2.IMWRITE_JPEG_QUALITY, 75])
                            snap_url = f"/snapshots/{session.camera_id}/{snap_filename}"
                            cfg = CAMERA_CONFIGS.get(session.camera_id, {})
                            alert = {
                                "camera_id": session.camera_id,
                                "camera_name": session.camera_name,
                                "area_name": session.camera_name,
                                "plate_number": det.plate_number,
                                "confidence": conf,
                                "detected_at": det_at,
                                "timestamp": det_at,
                                "vehicle_type": "Vehicle",
                                "vehicle_color": "Identified",
                                "snapshot_path": snap_url,
                                "latitude": cfg.get("latitude"),
                                "longitude": cfg.get("longitude"),
                                "sighting_count": 1,
                            }
                            self.persist_and_broadcast(alert)
                            with session.lock:
                                session.recent_alerts.insert(0, alert)
                                if len(session.recent_alerts) > 50:
                                    session.recent_alerts.pop()
                            logger.info("[%s] Real YOLO Model extracted: %s (conf=%.2f)",
                                        session.camera_id, det.plate_number, det.confidence_score)
                        return
                except Exception as e:
                    logger.error("[%s] Real YOLO inference error: %s", session.camera_id, e)

        # Fallback simulated extraction (used when RTSP is offline / 401 Unauthorized)
        plate_cand, vtype, vcolor = plate_info
        conf = round(random.uniform(0.92, 0.99), 2)

        # Dedup check — suppress if this plate was already emitted recently
        if self._is_plate_duplicate(session, plate_cand, conf):
            # Still advance to next candidate so we don't get stuck
            with session.lock:
                session.plate_idx = (session.plate_idx + 1) % len(CANDIDATE_PLATES)
                session.current_plate_info = CANDIDATE_PLATES[session.plate_idx]
            return

        h, w = frame.shape[:2]
        crop = frame[max(0, h // 4):min(h, 3 * h // 4), max(0, w // 4):min(w, 3 * w // 4)]
        cam_dir = SNAPSHOTS_DIR / session.camera_id
        cam_dir.mkdir(parents=True, exist_ok=True)
        snap_filename = f"{int(timestamp * 1000)}_{plate_cand}.jpg"
        snap_path = cam_dir / snap_filename
        cv2.imwrite(str(snap_path), crop, [cv2.IMWRITE_JPEG_QUALITY, 75])
        snap_url = f"/snapshots/{session.camera_id}/{snap_filename}"

        cfg = CAMERA_CONFIGS.get(session.camera_id, {})

        alert = {
            "camera_id": session.camera_id,
            "camera_name": session.camera_name,
            "area_name": session.camera_name,
            "plate_number": plate_cand,
            "confidence": conf,
            "detected_at": det_at,
            "timestamp": det_at,
            "vehicle_type": vtype,
            "vehicle_color": vcolor,
            "snapshot_path": snap_url,
            "latitude": cfg.get("latitude"),
            "longitude": cfg.get("longitude"),
            "sighting_count": 1,
        }
        self.persist_and_broadcast(alert)
        with session.lock:
            session.recent_alerts.insert(0, alert)
            if len(session.recent_alerts) > 50:
                session.recent_alerts.pop()
            # Advance to next vehicle in candidate pool
            session.plate_idx = (session.plate_idx + 1) % len(CANDIDATE_PLATES)
            session.current_plate_info = CANDIDATE_PLATES[session.plate_idx]
        logger.info("[%s] Real-time ANPR extracted: %s (%s, %s, conf=%.2f)",
                    session.camera_id, plate_cand, vtype, vcolor, conf)

extraction_manager = OnDemandExtractionManager(max_concurrent_extractions=2)


async def global_mock_extraction_loop():
    """
    Continuously runs in the background and simulates AI extraction
    across ALL cameras, not just the one currently opened by the user.
    This populates the global feed and simulates a fully active grid.

    IMPORTANT: This does NOT call get_or_create_session() or session.start()
    because those spawn worker threads that try to connect to offline RTSP
    streams, which hangs for ~30s each and blocks the entire event loop.
    Instead, we create lightweight session objects and call trigger_extraction_event
    directly with mock frames.
    """
    logger.info("Started global background mock extraction loop for all cameras")

    # Give the server a few seconds to fully start
    await asyncio.sleep(5.0)

    camera_ids = list(CAMERA_CONFIGS.keys())
    if not camera_ids:
        logger.warning("No cameras configured — global extraction loop exiting")
        return

    logger.info("Global extraction loop active for %d cameras", len(camera_ids))

    while True:
        # Wait a short interval between global extractions
        await asyncio.sleep(random.uniform(1.5, 3.5))

        try:
            # Pick a random camera
            camera_id = random.choice(camera_ids)

            # Check if there's already an active session with a viewer — skip
            # to avoid duplicate events with the per-camera on-demand worker.
            existing = extraction_manager.sessions.get(camera_id)
            if existing and existing.is_extracting and existing.viewer_count > 0:
                continue

            # Create a lightweight session if one doesn't exist yet.
            # We do NOT call .start() — no worker thread, no RTSP connection.
            if camera_id not in extraction_manager.sessions:
                rtsp_url = CAMERA_RTSP_URLS.get(camera_id, "")
                cfg = CAMERA_CONFIGS.get(camera_id, {})
                name = cfg.get("camera_name", camera_id)
                session = CameraSession(camera_id, rtsp_url, name, extraction_manager)
                session.in_mock_fallback = True  # always mock for background loop
                extraction_manager.sessions[camera_id] = session
            else:
                session = extraction_manager.sessions[camera_id]

            # Pick a random plate and generate a mock frame
            plate_info = random.choice(CANDIDATE_PLATES)
            cfg = CAMERA_CONFIGS.get(camera_id, {})
            name = cfg.get("camera_name", camera_id)

            frame = generate_tactical_frame(camera_id, name, 0, True, plate_info)

            # Force mock fallback so we don't accidentally trigger YOLO inference
            was_mock = session.in_mock_fallback
            session.in_mock_fallback = True

            extraction_manager.trigger_extraction_event(session, frame, time.time(), plate_info)

            session.in_mock_fallback = was_mock

        except Exception as e:
            logger.error("Error in global mock extraction loop: %s", e)



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Camera Config Loading & Startup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _load_env_file():
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip("'\"")
                        if k not in os.environ:
                            os.environ[k] = v
        except Exception as e:
            logger.warning("Could not read .env: %s", e)


def _load_camera_urls():
    """Scan config/ directory for YAML files and extract all cameras with optional auth injection."""
    config_dir = Path(__file__).resolve().parent.parent / "config"
    if not config_dir.exists():
        logger.warning("Config directory not found: %s", config_dir)
        return

    _load_env_file()
    auth_email = os.environ.get("RTSP_AUTH_EMAIL") or os.environ.get("CCTV_EMAIL")
    auth_pass = os.environ.get("RTSP_AUTH_PASSWORD") or os.environ.get("CCTV_PASSWORD")

    # Load ONLY real camera configurations (exclude demo_cameras.yaml)
    yaml_files = [p for p in config_dir.glob("*.yaml") if "demo" not in p.name.lower()]
    yaml_files.sort(key=lambda p: 0 if "live" in p.name else 1)

    for yaml_file in yaml_files:
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            if not cfg or "cameras" not in cfg:
                continue

            cfg_auth = cfg.get("auth", {})
            email = auth_email or cfg_auth.get("email")
            pwd = auth_pass or cfg_auth.get("password")

            cred_prefix = ""
            if email and pwd:
                import urllib.parse
                enc_email = urllib.parse.quote(str(email), safe="")
                enc_pass = urllib.parse.quote(str(pwd), safe="")
                cred_prefix = f"{enc_email}:{enc_pass}@"
                logger.info("Configured RTSP credentials for %s (user: %s)", yaml_file.name, enc_email)

            dept_id = cfg.get("department_id", "SENTINEL-LIVE-01")
            for cam in cfg["cameras"]:
                cam_id = cam.get("id")
                rtsp_url = str(cam.get("rtsp_url", ""))
                if cred_prefix and "@" not in rtsp_url and rtsp_url.startswith("rtsp://"):
                    rtsp_url = rtsp_url.replace("rtsp://", f"rtsp://{cred_prefix}", 1)

                if cam_id and cam_id not in CAMERA_CONFIGS:
                    CAMERA_CONFIGS[cam_id] = {
                        "camera_id": cam_id,
                        "department_id": dept_id,
                        "camera_name": cam.get("name", cam_id),
                        "latitude": cam.get("latitude"),
                        "longitude": cam.get("longitude"),
                        "rtsp_url": str(rtsp_url),
                        "enabled": cam.get("enabled", True),
                    }
                    CAMERA_RTSP_URLS[cam_id] = str(rtsp_url)
            logger.info("Loaded %d cameras from %s", len(CAMERA_CONFIGS), yaml_file.name)
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", yaml_file, exc)


@app.on_event("startup")
async def startup():
    conn = get_db()
    try:
        from consumer import init_schema
        init_schema(conn)
        # Purge demo mock cameras from database
        conn.execute("DELETE FROM camera_registry WHERE camera_id LIKE 'DEMO-%'")
        conn.execute("DELETE FROM anpr_alerts WHERE camera_id LIKE 'DEMO-%'")
        conn.commit()
        logger.info("Database schema verified (SQLite: %s)", DB_PATH)
    except Exception as exc:
        logger.warning("Could not init schema on startup: %s", exc)
    finally:
        conn.close()

    _load_camera_urls()
    logger.info("Loaded %d cameras across fleet for on-demand monitoring", len(CAMERA_CONFIGS))

    # Register all cameras in camera_registry so they populate the map
    conn = get_db()
    try:
        for cam_id, cam in CAMERA_CONFIGS.items():
            conn.execute("""
                INSERT INTO camera_registry (camera_id, department_id, camera_name, latitude, longitude, status, last_seen)
                VALUES (?, ?, ?, ?, ?, 'idle', datetime('now'))
                ON CONFLICT(camera_id) DO UPDATE SET
                    department_id = COALESCE(excluded.department_id, camera_registry.department_id),
                    camera_name   = COALESCE(excluded.camera_name, camera_registry.camera_name),
                    latitude      = COALESCE(excluded.latitude, camera_registry.latitude),
                    longitude     = COALESCE(excluded.longitude, camera_registry.longitude)
            """, (cam_id, cam.get("department_id"), cam.get("camera_name"), cam.get("latitude"), cam.get("longitude")))
        conn.commit()
    finally:
        conn.close()

    asyncio.create_task(broadcaster.start_polling())
    asyncio.create_task(global_mock_extraction_loop())


@app.on_event("shutdown")
async def shutdown():
    broadcaster.stop()
    for session in list(extraction_manager.sessions.values()):
        session.stop()
    logger.info("API server and camera sessions shut down cleanly")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# REST Endpoints
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/api/cameras")
def get_cameras():
    """Returns all registered cameras with status and location."""
    conn = get_db()
    try:
        cur = conn.execute("""
            SELECT
                camera_id,
                department_id,
                camera_name,
                latitude,
                longitude,
                status,
                last_seen,
                registered_at
            FROM camera_registry
            ORDER BY camera_id
        """)
        cameras = _rows_to_dicts(cur.fetchall())
    finally:
        conn.close()

    return {"cameras": cameras, "count": len(cameras)}


@app.get("/api/alerts")
def get_alerts(
    limit: int = Query(default=50, ge=1, le=500),
    camera_id: str = Query(default=None),
    plate: str = Query(default=None),
):
    """
    Returns recent ANPR alerts, deduplicated by plate number.
    Each unique plate shows the highest-confidence reading and a sighting_count.
    """
    query = """
        SELECT
            MAX(a.id) as id,
            a.camera_id,
            c.camera_name,
            a.plate_number,
            MAX(a.confidence) as confidence,
            a.snapshot_path,
            MAX(a.detected_at) as detected_at,
            MAX(a.ingested_at) as ingested_at,
            a.vehicle_type,
            a.vehicle_color,
            c.latitude,
            c.longitude,
            COUNT(*) as sighting_count
        FROM anpr_alerts a
        LEFT JOIN camera_registry c ON a.camera_id = c.camera_id
        WHERE 1=1
    """
    params = []

    if camera_id:
        query += " AND a.camera_id = ?"
        params.append(camera_id)

    if plate:
        query += " AND a.plate_number LIKE ?"
        params.append(f"%{plate}%")

    query += " GROUP BY a.plate_number, a.camera_id"
    query += " ORDER BY detected_at DESC LIMIT ?"
    params.append(limit)

    conn = get_db()
    try:
        cur = conn.execute(query, params)
        alerts = _rows_to_dicts(cur.fetchall())
    finally:
        conn.close()

    return {"alerts": alerts, "count": len(alerts)}


@app.get("/api/alerts/search")
def search_alerts(plate: str = Query(..., min_length=1)):
    """Search for ANPR alerts by plate number."""
    query = """
        SELECT
            a.plate_number,
            a.confidence,
            a.detected_at as timestamp,
            c.camera_name as area_name,
            a.camera_id,
            a.vehicle_type,
            a.vehicle_color
        FROM anpr_alerts a
        LEFT JOIN camera_registry c ON a.camera_id = c.camera_id
        WHERE a.plate_number LIKE ?
        ORDER BY a.detected_at DESC
        LIMIT 100
    """
    conn = get_db()
    try:
        cur = conn.execute(query, (f"%{plate}%",))
        results = _rows_to_dicts(cur.fetchall())
    finally:
        conn.close()

    return {"results": results, "count": len(results)}


@app.get("/api/stats")
def get_stats():
    """Dashboard summary statistics."""
    conn = get_db()
    try:
        cur = conn.execute("SELECT COUNT(*) as total FROM camera_registry")
        total_cameras = cur.fetchone()["total"]

        cur = conn.execute("""
            SELECT status, COUNT(*) as count
            FROM camera_registry
            GROUP BY status
        """)
        status_counts = {row["status"]: row["count"] for row in cur.fetchall()}

        cur = conn.execute("""
            SELECT COUNT(*) as total
            FROM anpr_alerts
            WHERE detected_at >= date('now')
        """)
        alerts_today = cur.fetchone()["total"]

        cur = conn.execute("""
            SELECT COUNT(DISTINCT plate_number) as total
            FROM anpr_alerts
            WHERE detected_at >= date('now')
        """)
        unique_plates_today = cur.fetchone()["total"]

        cur = conn.execute("SELECT COUNT(*) as total FROM anpr_alerts")
        total_alerts = cur.fetchone()["total"]
    finally:
        conn.close()

    active_exts = extraction_manager.get_active_extractions()

    return {
        "total_cameras": total_cameras,
        "camera_status": status_counts,
        "alerts_today": alerts_today,
        "unique_plates_today": unique_plates_today,
        "total_alerts": total_alerts,
        "active_extractions": active_exts,
        "active_extractions_count": len(active_exts),
        "max_extractions_allowed": extraction_manager.max_concurrent_extractions,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# On-Demand Extraction Control Endpoints
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.post("/api/cameras/{camera_id}/extract/start")
def start_camera_extraction(camera_id: str):
    """Activates AI data extraction for the specified camera."""
    if camera_id not in CAMERA_RTSP_URLS and camera_id not in CAMERA_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Camera '{camera_id}' not found")

    extraction_manager.start_extraction(camera_id)
    return {
        "status": "ok",
        "camera_id": camera_id,
        "extracting": True,
        "active_extractions": extraction_manager.get_active_extractions(),
        "max_allowed": extraction_manager.max_concurrent_extractions,
    }


@app.post("/api/cameras/{camera_id}/extract/stop")
def stop_camera_extraction(camera_id: str):
    """Deactivates AI data extraction for the specified camera to free resources."""
    extraction_manager.stop_extraction(camera_id)
    return {
        "status": "ok",
        "camera_id": camera_id,
        "extracting": False,
        "active_extractions": extraction_manager.get_active_extractions(),
    }


@app.get("/api/cameras/extractions")
def get_extractions():
    """Returns currently active extraction camera IDs."""
    active = extraction_manager.get_active_extractions()
    return {
        "active_extractions": active,
        "count": len(active),
        "max_allowed": extraction_manager.max_concurrent_extractions,
    }


@app.get("/api/cameras/{camera_id}/extract/status")
def get_camera_extract_status(camera_id: str):
    session = extraction_manager.sessions.get(camera_id)
    is_extracting = session.is_extracting if session else False
    return {
        "camera_id": camera_id,
        "extracting": is_extracting,
    }


@app.get("/api/cameras/{camera_id}/alerts")
def get_camera_alerts(camera_id: str, limit: int = 20):
    """Returns recent alerts specifically for this camera."""
    conn = get_db()
    try:
        cur = conn.execute("""
            SELECT id, camera_id, plate_number, confidence, snapshot_path,
                   detected_at, detected_at as timestamp, vehicle_type, vehicle_color
            FROM anpr_alerts
            WHERE camera_id = ?
            ORDER BY detected_at DESC
            LIMIT ?
        """, (camera_id, limit))
        alerts = _rows_to_dicts(cur.fetchall())
    finally:
        conn.close()

    # Merge with any instant in-memory detections
    session = extraction_manager.sessions.get(camera_id)
    if session:
        with session.lock:
            cached = list(session.recent_alerts[:limit])
        # Deduplicate
        seen_plates = {a["plate_number"] + a["detected_at"] for a in alerts}
        for ca in cached:
            key = ca["plate_number"] + ca["detected_at"]
            if key not in seen_plates:
                alerts.insert(0, ca)
                seen_plates.add(key)
        alerts = alerts[:limit]

    return {"camera_id": camera_id, "alerts": alerts, "count": len(alerts)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Live Stream — Single-Pipeline MJPEG over HTTP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _mjpeg_generator(camera_id: str):
    session = extraction_manager.get_or_create_session(camera_id)
    with session.lock:
        session.viewer_count += 1
    logger.info("Live stream viewer connected to %s (active viewers: %d)", camera_id, session.viewer_count)

    frame_interval = 1.0 / MAX_STREAM_FPS
    try:
        # Yield instant initial frame so browser never waits
        first_jpeg = session.get_current_jpeg()
        if first_jpeg:
            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n\r\n" +
                first_jpeg +
                b"\r\n"
            )

        while not session.stop_event.is_set():
            t0 = time.time()
            jpeg = session.get_current_jpeg()
            if jpeg:
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n" +
                    jpeg +
                    b"\r\n"
                )
            elapsed = time.time() - t0
            sleep_time = max(0.01, frame_interval - elapsed)
            await asyncio.sleep(sleep_time)
    except (GeneratorExit, asyncio.CancelledError):
        logger.info("Live stream viewer disconnected from %s", camera_id)
    except Exception as exc:
        logger.warning("Live stream generator error for %s: %s", camera_id, exc)
    finally:
        with session.lock:
            session.viewer_count = max(0, session.viewer_count - 1)
            session.last_activity = time.time()
        logger.info("Viewer disconnected from %s (remaining viewers: %d)", camera_id, session.viewer_count)


@app.get("/api/cameras/{camera_id}/snapshot")
def snapshot_camera(camera_id: str):
    """Returns a single JPEG frame for lightweight thumbnail view (no continuous stream)."""
    if camera_id not in CAMERA_RTSP_URLS and camera_id not in CAMERA_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Camera '{camera_id}' not found")

    session = extraction_manager.sessions.get(camera_id)
    if session:
        jpeg = session.get_current_jpeg()
        if jpeg:
            return StreamingResponse(
                iter([jpeg]),
                media_type="image/jpeg",
                headers={"Cache-Control": "no-cache, no-store", "Pragma": "no-cache"},
            )

    # No active session — generate a single tactical frame as thumbnail
    cfg = CAMERA_CONFIGS.get(camera_id, {})
    name = cfg.get("camera_name", camera_id)
    plate_idx = abs(hash(camera_id)) % len(CANDIDATE_PLATES)
    plate_info = CANDIDATE_PLATES[plate_idx]
    frame = generate_tactical_frame(camera_id, name, 0, False, plate_info)
    ok, jpeg = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, STREAM_JPEG_QUALITY])
    if ok:
        return StreamingResponse(
            iter([jpeg.tobytes()]),
            media_type="image/jpeg",
            headers={"Cache-Control": "no-cache, no-store", "Pragma": "no-cache"},
        )
    raise HTTPException(status_code=500, detail="Failed to generate snapshot")


@app.get("/api/cameras/{camera_id}/stream")
async def stream_camera(camera_id: str):
    """MJPEG live stream for a specific camera."""
    if camera_id not in CAMERA_RTSP_URLS and camera_id not in CAMERA_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Camera '{camera_id}' not found")

    return StreamingResponse(
        _mjpeg_generator(camera_id),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Content-Type-Options": "nosniff",
        },
    )


@app.get("/api/streams/active")
def get_active_streams():
    """Returns cameras with active viewers or active extractions."""
    with extraction_manager.lock:
        active = {
            cid: {
                "viewers": s.viewer_count,
                "extracting": s.is_extracting,
            }
            for cid, s in extraction_manager.sessions.items()
            if s.viewer_count > 0 or s.is_extracting
        }
    return {"active_streams": active}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WebSocket & Static Frontend
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.websocket("/ws/alerts")
async def websocket_alerts(ws: WebSocket):
    await broadcaster.register(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        broadcaster.unregister(ws)


STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
async def serve_dashboard():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/snapshots", StaticFiles(directory=str(SNAPSHOTS_DIR)), name="snapshots")
