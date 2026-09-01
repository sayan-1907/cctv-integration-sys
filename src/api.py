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
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
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
        from consumer import SCHEMA_SQL
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        logger.info("Database schema verified (SQLite: %s)", DB_PATH)
    except Exception as exc:
        logger.warning("Could not init schema on startup: %s", exc)
    finally:
        conn.close()


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


@app.get("/api/stats")
def get_stats():
    """
    Dashboard summary statistics — total cameras, alerts today,
    unique plates today, and cameras by status.
    """
    conn = get_db()
    try:
        # Total cameras
        cur = conn.execute("SELECT COUNT(*) as total FROM camera_registry")
        total_cameras = cur.fetchone()["total"]

        # Camera status breakdown
        cur = conn.execute("""
            SELECT status, COUNT(*) as count
            FROM camera_registry
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
                    a.detected_at, c.latitude, c.longitude
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
