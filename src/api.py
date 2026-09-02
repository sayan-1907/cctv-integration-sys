"""
api.py — Layer 4: Dashboard API & Static File Server

A FastAPI application that serves:
  1. REST endpoints for the dashboard to query cameras and alerts
     from the local SQLite database.
  2. A WebSocket endpoint for real-time alert streaming — the
     dashboard gets push updates instead of polling.
  3. Static files for the frontend dashboard (HTML/CSS/JS).

Design decisions:
  - The API reads from the SAME SQLite database that consumer.py
    writes to. They share nothing except the database — no in-process
    coupling, either can be restarted independently.
  - SQLite WAL mode allows concurrent readers + single writer,
    which is sufficient for the hackathon demo.
  - The WebSocket endpoint polls the database every 2 seconds for new
    alerts and pushes them. This is simpler than a Kafka consumer inside
    the web server (which would need its own consumer group management)
    and good enough for a hackathon dashboard refresh rate.

Usage:
    uvicorn api:app --host 0.0.0.0 --port 8000 --reload
"""

import json
import time
import sqlite3
import asyncio
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager
from typing import Optional

import cv2
import yaml

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

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
    description="Layer 4 — Camera monitoring & ANPR alert dashboard",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database path — same file consumer.py writes to
DB_PATH = str(Path(__file__).resolve().parent.parent / "sentinel.db")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Camera RTSP URL lookup (loaded from YAML config at startup)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Maps camera_id -> rtsp_url for the live stream endpoint.
# Populated once at startup from all YAML configs found in config/.
CAMERA_RTSP_URLS: dict[str, str] = {}

# Tracks active stream threads so we can monitor/limit them
_active_streams: dict[str, int] = {}  # camera_id -> viewer count
_stream_lock = threading.Lock()
MAX_STREAM_FPS = 10
STREAM_JPEG_QUALITY = 65  # 0-100, lower = smaller frames = less bandwidth


def _load_camera_urls():
    """Scan config/ directory for YAML files and extract camera RTSP URLs."""
    config_dir = Path(__file__).resolve().parent.parent / "config"
    if not config_dir.exists():
        logger.warning("Config directory not found: %s", config_dir)
        return

    for yaml_file in config_dir.glob("*.yaml"):
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            if not cfg or "cameras" not in cfg:
                continue
            for cam in cfg["cameras"]:
                cam_id = cam.get("id")
                rtsp_url = cam.get("rtsp_url")
                if cam_id and rtsp_url and cam.get("enabled", True):
                    # Skip mock streams (they aren't real RTSP)
                    if isinstance(rtsp_url, str) and not rtsp_url.startswith("mock://"):
                        CAMERA_RTSP_URLS[cam_id] = rtsp_url
            logger.info("Loaded %d camera URLs from %s", len(CAMERA_RTSP_URLS), yaml_file.name)
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", yaml_file, exc)


def get_db() -> sqlite3.Connection:
    """
    Open a new SQLite connection for each request.
    Using sqlite3.Row as the row_factory gives us dict-like access
    by column name.
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@app.on_event("startup")
async def startup():
    # Ensure tables exist even if the consumer hasn't run yet
    conn = get_db()
    try:
        from consumer import init_schema
        init_schema(conn)
        logger.info("Database schema verified (SQLite: %s)", DB_PATH)
    except Exception as exc:
        logger.warning("Could not init schema on startup: %s", exc)
    finally:
        conn.close()

    # Load camera RTSP URLs for the live stream endpoint
    _load_camera_urls()
    logger.info("Camera RTSP URL lookup ready: %d cameras available for live streaming", len(CAMERA_RTSP_URLS))


@app.on_event("shutdown")
async def shutdown():
    logger.info("API server shutting down")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Helper — convert sqlite3.Row to a plain dict
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _rows_to_dicts(rows) -> list[dict]:
    """Convert a list of sqlite3.Row objects to plain dicts for JSON."""
    return [dict(row) for row in rows]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# REST endpoints
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@app.get("/api/cameras")
def get_cameras():
    """
    Returns all registered cameras with their latest status and location.
    Used by the dashboard map layer to plot camera markers.
    """
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
            WHERE camera_id NOT LIKE 'DEMO-%'
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
    Returns recent ANPR alerts, optionally filtered by camera or plate.
    Ordered newest-first.
    """
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
        WHERE a.camera_id NOT LIKE 'DEMO-%'
    """
    params = []

    if camera_id:
        query += " AND a.camera_id = ?"
        params.append(camera_id)

    if plate:
        # SQLite LIKE is case-insensitive for ASCII by default
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
    """
    Search for ANPR alerts by plate number (partial, case-insensitive).
    Returns detailed objects for the detail view modal.
    """
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
        WHERE a.camera_id NOT LIKE 'DEMO-%'
          AND a.plate_number LIKE ?
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
    """
    Dashboard summary statistics — total cameras, alerts today,
    unique plates today, and cameras by status.
    """
    conn = get_db()
    try:
        # Total cameras
        cur = conn.execute("SELECT COUNT(*) as total FROM camera_registry WHERE camera_id NOT LIKE 'DEMO-%'")
        total_cameras = cur.fetchone()["total"]

        # Camera status breakdown
        cur = conn.execute("""
            SELECT status, COUNT(*) as count
            FROM camera_registry
            WHERE camera_id NOT LIKE 'DEMO-%'
            GROUP BY status
        """)
        status_counts = {row["status"]: row["count"] for row in cur.fetchall()}

        # Alerts today
        cur = conn.execute("""
            SELECT COUNT(*) as total
            FROM anpr_alerts
            WHERE detected_at >= date('now')
        """)
        alerts_today = cur.fetchone()["total"]

        # Unique plates today
        cur = conn.execute("""
            SELECT COUNT(DISTINCT plate_number) as total
            FROM anpr_alerts
            WHERE detected_at >= date('now')
        """)
        unique_plates_today = cur.fetchone()["total"]

        # Total alerts all time
        cur = conn.execute("SELECT COUNT(*) as total FROM anpr_alerts")
        total_alerts = cur.fetchone()["total"]
    finally:
        conn.close()

    return {
        "total_cameras": total_cameras,
        "camera_status": status_counts,
        "alerts_today": alerts_today,
        "unique_plates_today": unique_plates_today,
        "total_alerts": total_alerts,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Live Stream — MJPEG over HTTP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _mjpeg_generator(camera_id: str, rtsp_url: str):
    """
    Generator that opens an independent cv2.VideoCapture connection
    to the camera's RTSP feed and yields JPEG-encoded frames as
    multipart MJPEG chunks.

    The generator runs in a synchronous thread (FastAPI handles this
    via StreamingResponse). When the client disconnects, the generator
    is garbage-collected and the finally block releases the capture.
    """
    cap = None
    frame_interval = 1.0 / MAX_STREAM_FPS

    with _stream_lock:
        _active_streams[camera_id] = _active_streams.get(camera_id, 0) + 1
    logger.info("Live stream started for %s (viewers: %d)", camera_id, _active_streams.get(camera_id, 0))

    try:
        cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
        if not cap.isOpened():
            logger.warning("Failed to open RTSP stream for %s: %s", camera_id, rtsp_url)
            return

        # Try to reduce capture buffer to minimize latency
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

        consecutive_failures = 0
        while True:
            t0 = time.time()

            ret, frame = cap.read()
            if not ret:
                consecutive_failures += 1
                if consecutive_failures > 30:  # ~3 seconds of failures
                    logger.warning("Stream %s: too many consecutive read failures, stopping", camera_id)
                    break
                time.sleep(0.1)
                continue
            consecutive_failures = 0

            # Encode to JPEG
            encode_params = [cv2.IMWRITE_JPEG_QUALITY, STREAM_JPEG_QUALITY]
            ok, jpeg = cv2.imencode(".jpg", frame, encode_params)
            if not ok:
                continue

            # Yield as multipart MJPEG frame
            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n\r\n" +
                jpeg.tobytes() +
                b"\r\n"
            )

            # Throttle to target FPS
            elapsed = time.time() - t0
            if elapsed < frame_interval:
                time.sleep(frame_interval - elapsed)

    except GeneratorExit:
        # Client disconnected — normal, clean exit
        logger.info("Live stream client disconnected for %s", camera_id)
    except Exception as exc:
        logger.warning("Live stream error for %s: %s", camera_id, exc)
    finally:
        if cap is not None:
            cap.release()
        with _stream_lock:
            count = _active_streams.get(camera_id, 1) - 1
            if count <= 0:
                _active_streams.pop(camera_id, None)
            else:
                _active_streams[camera_id] = count
        logger.info("Live stream ended for %s", camera_id)


@app.get("/api/cameras/{camera_id}/stream")
def stream_camera(camera_id: str):
    """
    MJPEG live stream for a specific camera.
    Returns a multipart/x-mixed-replace response that browsers
    can display directly in an <img> tag.
    """
    rtsp_url = CAMERA_RTSP_URLS.get(camera_id)
    if not rtsp_url:
        raise HTTPException(status_code=404, detail=f"Camera '{camera_id}' not found or has no RTSP URL")

    return StreamingResponse(
        _mjpeg_generator(camera_id, rtsp_url),
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
    """Returns which cameras currently have active live stream viewers."""
    with _stream_lock:
        return {"active_streams": dict(_active_streams)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WebSocket — real-time alert push
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class AlertBroadcaster:
    """
    Polls the database for new alerts and broadcasts them to all
    connected WebSocket clients. Simpler than routing Kafka messages
    directly into the web server process.
    """

    def __init__(self):
        self.clients: list[WebSocket] = []
        self._last_id = 0
        self._running = False

    async def register(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)
        logger.info("WebSocket client connected (%d total)", len(self.clients))

    def unregister(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)
        logger.info("WebSocket client disconnected (%d remaining)", len(self.clients))

    async def start_polling(self):
        """Background task that polls for new alerts every 2 seconds."""
        if self._running:
            return
        self._running = True

        # Initialize _last_id to the current max
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
            await asyncio.sleep(2.0)
            if not self.clients:
                continue

            try:
                new_alerts = self._fetch_new_alerts()
                if new_alerts:
                    message = json.dumps({"type": "new_alerts", "alerts": new_alerts})
                    dead = []
                    for ws in self.clients:
                        try:
                            await ws.send_text(message)
                        except Exception:
                            dead.append(ws)
                    for ws in dead:
                        self.unregister(ws)
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
                    a.detected_at, a.vehicle_type, a.vehicle_color,
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


@app.on_event("startup")
async def start_broadcaster():
    asyncio.create_task(broadcaster.start_polling())


@app.websocket("/ws/alerts")
async def websocket_alerts(ws: WebSocket):
    await broadcaster.register(ws)
    try:
        while True:
            # Keep the connection alive — client can send pings
            await ws.receive_text()
    except WebSocketDisconnect:
        broadcaster.unregister(ws)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Static file serving — dashboard frontend
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
async def serve_dashboard():
    return FileResponse(STATIC_DIR / "index.html")


# Mount static files AFTER specific routes so / doesn't get hijacked
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
