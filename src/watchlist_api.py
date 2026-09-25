import sqlite3
from pathlib import Path
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import logging

logger = logging.getLogger("layer4.routes.watchlist")
router = APIRouter()

DB_PATH = str(Path(__file__).resolve().parent.parent / "sentinel.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_watchlist_schema():
    """
    Create the watchlist_vehicles table if it does not exist.
    Called by api.py on_event('startup') — NOT via @router.on_event,
    because APIRouter instances cannot own lifecycle events.
    """
    try:
        conn = get_db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist_vehicles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plate_number TEXT UNIQUE NOT NULL,
                reason TEXT,
                priority TEXT DEFAULT 'high',
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            );
        """)
        conn.commit()
        conn.close()
        logger.info("Watchlist schema initialised in SQLite")
    except Exception as e:
        logger.error(f"Failed to init watchlist schema: {e}")


class WatchlistTarget(BaseModel):
    plate_number: str
    reason: str = "suspicious"
    priority: str = "high"


@router.get("/api/watchlist")
def get_watchlist():
    """Return all active watchlist targets."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT plate_number, reason, priority, added_at "
            "FROM watchlist_vehicles WHERE is_active = 1 ORDER BY added_at DESC"
        )
        targets = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()
        return {"targets": targets}
    except Exception as e:
        logger.error(f"Error fetching watchlist: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")


@router.post("/api/watchlist")
def add_to_watchlist(target: WatchlistTarget):
    """Add or re-activate a plate on the watchlist."""
    try:
        conn = get_db()
        plate = target.plate_number.strip().upper()
        conn.execute("""
            INSERT INTO watchlist_vehicles (plate_number, reason, priority)
            VALUES (?, ?, ?)
            ON CONFLICT(plate_number) DO UPDATE SET
                is_active = 1,
                reason = excluded.reason,
                priority = excluded.priority,
                added_at = CURRENT_TIMESTAMP
        """, (plate, target.reason, target.priority.lower()))
        conn.commit()
        conn.close()
        return {"status": "success", "plate_number": plate}
    except Exception as e:
        logger.error(f"Error adding to watchlist: {e}")
        raise HTTPException(status_code=500, detail="Database insert failed")


@router.delete("/api/watchlist/{plate}")
def remove_from_watchlist(plate: str):
    """Soft-delete a plate from the watchlist (is_active = 0)."""
    try:
        conn = get_db()
        conn.execute(
            "UPDATE watchlist_vehicles SET is_active = 0 WHERE plate_number = ?",
            (plate.strip().upper(),)
        )
        conn.commit()
        conn.close()
        return {"status": "success", "plate_number": plate}
    except Exception as e:
        logger.error(f"Error removing from watchlist: {e}")
        raise HTTPException(status_code=500, detail="Database update failed")
