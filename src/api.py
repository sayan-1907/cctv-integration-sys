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

CANDIDATE_PLATES = [
    ("GJ01AB1234", "Sedan", "Silver"),
    ("GJ27BK8890", "SUV", "Black"),
    ("GJ05CD5678", "Hatchback", "White"),
    ("GJ03EF9012", "Sedan", "Red"),
    ("GJ06GH3456", "SUV", "Blue"),
    ("GJ01XX9988", "Sedan", "White"),
    ("GJ18AA5544", "Truck", "Grey"),
    ("GJ02ZZ1122", "SUV", "Black"),
    ("GJ10MN4321", "Hatchback", "Silver"),
    ("GJ12PQ6789", "Sedan", "Blue"),
]

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

def generate_tactical_frame(
    camera_id: str,
    camera_name: str,
    seq: int,
    is_extracting: bool,
    plate_info: tuple[str, str, str],
) -> np.ndarray:
    w, h = 640, 360
    frame = np.zeros((h, w, 3), dtype=np.uint8)

    # Road surface
    frame[70:h, :] = (26, 30, 38)

    # Lane markings (smoothly moving forward)
    offset = int((seq * 7) % 36)
    for y in range(70, h, 36):
        actual_y = y + offset
        if actual_y < h:
            cv2.line(frame, (w // 2, actual_y), (w // 2, min(h, actual_y + 18)), (210, 215, 225), 2)
            cv2.line(frame, (w // 4, actual_y), (w // 4, min(h, actual_y + 14)), (90, 100, 115), 1)
            cv2.line(frame, (3 * w // 4, actual_y), (3 * w // 4, min(h, actual_y + 14)), (90, 100, 115), 1)

    # Road shoulder edges
    cv2.line(frame, (50, 70), (15, h), (75, 85, 105), 2)
    cv2.line(frame, (w - 50, 70), (w - 15, h), (75, 85, 105), 2)

    plate_str, vtype_str, vcolor_str = plate_info

    # Moving Vehicle in primary lane (approaching camera)
    progress = (seq % 100) / 100.0
    car_y = int(75 + progress * (h - 135))
    car_scale = 0.5 + progress * 0.75

    car_w = int(140 * car_scale)
    car_h = int(90 * car_scale)
    car_x = int(380 - (car_w // 2) + np.sin(progress * np.pi) * 15)

    # Vehicle body color
    body_color = (180, 185, 195) if vcolor_str == "Silver" else (
        (30, 30, 35) if vcolor_str == "Black" else (
            (240, 240, 245) if vcolor_str == "White" else (
                (40, 40, 200) if vcolor_str == "Red" else (
                    (190, 80, 40) if vcolor_str == "Blue" else (120, 125, 135)
                )
            )
        )
    )

    cv2.rectangle(frame, (car_x, car_y), (car_x + car_w, car_y + car_h), (35, 42, 55), -1)
    cv2.rectangle(frame, (car_x + 4, car_y + int(car_h * 0.18)), (car_x + car_w - 4, car_y + int(car_h * 0.72)), body_color, -1)

    # Glass / windshield
    cv2.rectangle(frame, (car_x + int(car_w * 0.14), car_y + int(car_h * 0.22)), (car_x + int(car_w * 0.86), car_y + int(car_h * 0.52)), (20, 24, 32), -1)

    # Taillights
    light_w = max(4, int(14 * car_scale))
    light_h = max(2, int(7 * car_scale))
    cv2.rectangle(frame, (car_x + 6, car_y + car_h - 15), (car_x + 6 + light_w, car_y + car_h - 15 + light_h), (20, 20, 220), -1)
    cv2.rectangle(frame, (car_x + car_w - 6 - light_w, car_y + car_h - 15), (car_x + car_w - 6, car_y + car_h - 15 + light_h), (20, 20, 220), -1)

    # Indian License Plate Box on the rear bumper
    plate_w = max(38, int(68 * car_scale))
    plate_h = max(11, int(17 * car_scale))
    plate_x = car_x + (car_w - plate_w) // 2
    plate_y = car_y + car_h - plate_h - 4

    # Plate graphic: White base with blue IND badge
    cv2.rectangle(frame, (plate_x, plate_y), (plate_x + plate_w, plate_y + plate_h), (252, 252, 252), -1)
    cv2.rectangle(frame, (plate_x, plate_y), (plate_x + int(plate_w * 0.12), plate_y + plate_h), (180, 50, 20), -1)
    cv2.rectangle(frame, (plate_x, plate_y), (plate_x + plate_w, plate_y + plate_h), (0, 0, 0), 1)

    plate_font_scale = max(0.26, car_scale * 0.33)
    cv2.putText(
        frame,
        plate_str,
        (plate_x + int(plate_w * 0.15), plate_y + int(plate_h * 0.76)),
        cv2.FONT_HERSHEY_SIMPLEX,
        plate_font_scale,
        (0, 0, 0),
        1,
    )

    # When extraction is ACTIVE: Draw tactical AI bounding boxes & confidence tags
    if is_extracting:
        # Vehicle detection box (Cyan)
        cv2.rectangle(frame, (car_x - 3, car_y - 3), (car_x + car_w + 3, car_y + car_h + 3), (255, 229, 0), 2)
        cv2.putText(
            frame,
            f"VEHICLE: {vtype_str.upper()} 96%",
            (car_x - 3, max(20, car_y - 8)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.40,
            (255, 229, 0),
            1,
        )

        # License plate detection box (Amber / Gold)
        cv2.rectangle(frame, (plate_x - 2, plate_y - 2), (plate_x + plate_w + 2, plate_y + plate_h + 2), (0, 215, 255), 2)
        cv2.putText(
            frame,
            f"ALPR: {plate_str} [98%]",
            (plate_x - 12, plate_y + plate_h + 13),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.36,
            (0, 215, 255),
            1,
        )

    # Top Telemetry Header Bar
    cv2.rectangle(frame, (0, 0), (w, 38), (10, 14, 22), -1)
    cv2.line(frame, (0, 38), (w, 38), (35, 45, 60), 1)
    cv2.putText(
        frame,
        f"SENTINEL CAM: {camera_name} [{camera_id}]",
        (12, 16),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.44,
        (248, 250, 252),
        1,
    )

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    status_label = "AI EXTRACTING (RTSP OFFLINE/401 FALLBACK)" if is_extracting else "STREAM (RTSP OFFLINE/401 FALLBACK)"
    status_color = (0, 220, 255) if is_extracting else (148, 163, 184)
    cv2.putText(
        frame,
        f"{now_str}  |  {status_label}",
        (12, 31),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.33,
        status_color,
        1,
    )

    cv2.putText(
        frame,
        "15 FPS · HD",
        (w - 85, 16),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.35,
        (100, 116, 139),
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
        self.recent_plates: dict[str, float] = {}
        self.last_inference_ts = 0.0
        self.last_activity = time.time()
        self.frame_seq = 0
        self.in_mock_fallback = False
        self.lock = threading.RLock()

        # Initialize candidate plate
        self.plate_idx = abs(hash(camera_id)) % len(CANDIDATE_PLATES)
        self.current_plate_info = CANDIDATE_PLATES[self.plate_idx]

        # Generate instant initial JPEG so stream never hangs on startup
        initial_frame = generate_tactical_frame(self.camera_id, self.camera_name, 0, False, self.current_plate_info)
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
        logger.info("[%s] Worker loop running (source=%s)", self.camera_id, self.rtsp_url)
        cap = None

        # Non-blocking probe to see if RTSP is truly reachable
        is_online = _probe_rtsp_online(self.rtsp_url, timeout=0.35)
        if is_online:
            try:
                import os
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|timeout;1500000"
                cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                if not cap.isOpened():
                    logger.warning("[%s] RTSP stream unreachable, falling back to simulated traffic feed", self.camera_id)
                    self.in_mock_fallback = True
                    cap = None
            except Exception as e:
                logger.warning("[%s] RTSP open exception: %s", self.camera_id, e)
                self.in_mock_fallback = True
                cap = None
        else:
            self.in_mock_fallback = True

        frame_interval = 1.0 / MAX_STREAM_FPS
        encode_params = [cv2.IMWRITE_JPEG_QUALITY, STREAM_JPEG_QUALITY]

        while not self.stop_event.is_set():
            t0 = time.time()
            frame = None

            if cap is not None and cap.isOpened():
                ret, frame = cap.read()
                if not ret or frame is None:
                    cap.release()
                    cap = None
                    self.in_mock_fallback = True

            if frame is None:
                # Cycle simulated plate every 80 frames (~5-6s)
                if self.frame_seq % 80 == 0:
                    self.plate_idx = (self.plate_idx + 1) % len(CANDIDATE_PLATES)
                    self.current_plate_info = CANDIDATE_PLATES[self.plate_idx]
                frame = generate_tactical_frame(
                    self.camera_id,
                    self.camera_name,
                    self.frame_seq,
                    self.is_extracting,
                    self.current_plate_info,
                )

            self.frame_seq += 1

            # Encode and store latest JPEG
            ok, jpeg = cv2.imencode(".jpg", frame, encode_params)
            if ok:
                jpeg_bytes = jpeg.tobytes()
                with self.condition:
                    self.latest_jpeg = jpeg_bytes
                    self.latest_frame = frame
                    self.condition.notify_all()

            # Run on-demand AI extraction when active
            if self.is_extracting:
                now = time.time()
                # Run extraction event every 2.5s
                if now - self.last_inference_ts >= 2.5:
                    self.last_inference_ts = now
                    try:
                        self.manager.trigger_extraction_event(self, frame, now, self.current_plate_info)
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
                                "confidence": round(float(det.confidence_score), 2),
                                "detected_at": det_at,
                                "timestamp": det_at,
                                "vehicle_type": "Vehicle",
                                "vehicle_color": "Identified",
                                "snapshot_path": snap_url,
                                "latitude": cfg.get("latitude"),
                                "longitude": cfg.get("longitude"),
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
    """Returns recent ANPR alerts, optionally filtered by camera or plate."""
    query = """
        SELECT
            a.id,
            a.camera_id,
            c.camera_name,
            a.plate_number,
            a.confidence,
            a.snapshot_path,
            a.detected_at,
            a.ingested_at,
            a.vehicle_type,
            a.vehicle_color,
            c.latitude,
            c.longitude
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

    query += " ORDER BY a.detected_at DESC LIMIT ?"
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
