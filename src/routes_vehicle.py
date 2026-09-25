import sqlite3
import requests
import math
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import logging

logger = logging.getLogger("layer4.routes.vehicle")
router = APIRouter()

DB_PATH = str(Path(__file__).resolve().parent.parent / "sentinel.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance in kilometers between two points on the earth."""
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    R = 6371.0 # Earth radius in km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

class TrackRequest(BaseModel):
    plate_number: str

@router.post("/api/vehicles/track")
def track_vehicle(req: TrackRequest):
    """
    Route Reconstruction: Fetches chronological sightings of a plate from SQLite, 
    calculates speeds/distances in Python, and queries OSRM for snapping.
    """
    plate = req.plate_number.strip().upper()
    
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # SQLite doesn't have ST_DistanceSphere, so we calculate in Python
        query = """
            SELECT 
                a.camera_id, 
                c.camera_name,
                c.latitude, 
                c.longitude,
                a.detected_at, 
                a.snapshot_path
            FROM anpr_alerts a
            JOIN camera_registry c ON a.camera_id = c.camera_id
            WHERE a.plate_number = ?
            ORDER BY a.detected_at ASC
        """
        cur.execute(query, (plate,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error querying vehicle route: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")
        
    if not rows:
        return {
            "plate": plate,
            "summary": {"total_distance_km": 0, "total_duration_min": 0, "avg_speed_kmh": 0, "confidence_score": "N/A"},
            "sightings": [],
            "route_segments": [],
            "geojson": None
        }

    sightings = []
    route_segments = []
    total_distance_km = 0.0
    total_duration_hours = 0.0
    has_anomaly = False

    # Convert to list of dicts for easier processing
    records = [dict(r) for r in rows]

    for i, row in enumerate(records):
        sightings.append({
            "camera_id": row["camera_id"],
            "name": row["camera_name"],
            "lat": row["latitude"],
            "lng": row["longitude"],
            "timestamp": row["detected_at"],
            "snapshot": row["snapshot_path"]
        })
        
        if i > 0:
            prev_row = records[i-1]
            
            # Distance
            dist_km = haversine(prev_row["latitude"], prev_row["longitude"], row["latitude"], row["longitude"])
            
            # Duration
            # Assuming detected_at is ISO8601 string like "2023-09-20T14:30:00+00:00"
            try:
                t1 = datetime.fromisoformat(prev_row["detected_at"].replace("Z", "+00:00"))
                t2 = datetime.fromisoformat(row["detected_at"].replace("Z", "+00:00"))
                dur_hrs = (t2 - t1).total_seconds() / 3600.0
            except ValueError:
                dur_hrs = 0
            
            if prev_row["camera_id"] != row["camera_id"]:
                if dur_hrs > 0:
                    speed_kmh = dist_km / dur_hrs
                else:
                    speed_kmh = 999
                    
                is_anomaly = speed_kmh > 160 or speed_kmh < 0
                if is_anomaly:
                    has_anomaly = True
                    
                total_distance_km += dist_km
                total_duration_hours += dur_hrs
                
                route_segments.append({
                    "from_cam": prev_row["camera_id"],
                    "to_cam": row["camera_id"],
                    "distance_km": round(dist_km, 2),
                    "duration_min": round(dur_hrs * 60, 1),
                    "speed_kmh": round(speed_kmh, 1),
                    "is_anomaly": is_anomaly
                })

    # OSRM Snapping
    geojson_line = None
    valid_coords = [s for s in sightings if s["lat"] is not None and s["lng"] is not None]
    if len(valid_coords) > 1:
        coords_str = ";".join([f"{s['lng']},{s['lat']}" for s in valid_coords])
        try:
            osrm_url = f"http://router.project-osrm.org/route/v1/driving/{coords_str}?geometries=geojson&overview=full"
            resp = requests.get(osrm_url, timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("routes"):
                    geojson_line = data["routes"][0]["geometry"]
        except Exception as e:
            logger.warning(f"OSRM snapping failed: {e}")

    avg_speed = (total_distance_km / total_duration_hours) if total_duration_hours > 0 else 0
    confidence = "Low" if has_anomaly else "High"
    
    return {
        "plate": plate,
        "summary": {
            "total_distance_km": round(total_distance_km, 2),
            "total_duration_min": round(total_duration_hours * 60, 1),
            "avg_speed_kmh": round(avg_speed, 1),
            "confidence_score": confidence
        },
        "sightings": sightings,
        "route_segments": route_segments,
        "geojson": geojson_line
    }
