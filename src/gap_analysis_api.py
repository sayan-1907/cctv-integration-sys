import sqlite3
import math
from pathlib import Path
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
import logging

logger = logging.getLogger("layer4.routes.gap_analysis")
router = APIRouter()

DB_PATH = str(Path(__file__).resolve().parent.parent / "sentinel.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    R = 6371.0 # Earth radius in km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

@router.get("/api/cameras/coverage-analysis")
def get_coverage_analysis():
    """
    GIS Gap Analysis (SQLite Fallback): Calculates approximate coverage area 
    and identifies gaps without PostGIS.
    """
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT camera_id, latitude, longitude FROM camera_registry WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        cameras = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        # Approximate covered area: N cameras * Area of 150m radius circle (0.07 km^2)
        # Assuming minimal overlap for this mock calculation
        total_covered_area_km2 = len(cameras) * (math.pi * (0.150 ** 2))
        
        coverage_gaps = []
        recommended_deployments = []
        
        # Find sparse pairs (distance > 800m and < 2000m)
        sparse_pairs = []
        for i in range(len(cameras)):
            for j in range(i + 1, len(cameras)):
                cam1, cam2 = cameras[i], cameras[j]
                dist_km = haversine(cam1['latitude'], cam1['longitude'], cam2['latitude'], cam2['longitude'])
                dist_m = dist_km * 1000
                if 800 <= dist_m <= 2000:
                    sparse_pairs.append({
                        "cam1": cam1['camera_id'],
                        "cam2": cam2['camera_id'],
                        "dist_m": dist_m,
                        "gap_lat": (cam1['latitude'] + cam2['latitude']) / 2.0,
                        "gap_lon": (cam1['longitude'] + cam2['longitude']) / 2.0,
                    })
        
        # Sort by distance descending, take top 3
        sparse_pairs.sort(key=lambda x: x["dist_m"], reverse=True)
        gaps = sparse_pairs[:3]
        
        for i, gap in enumerate(gaps):
            coverage_gaps.append({
                "gap_id": f"GAP-{i+1:03d}",
                "location": {"lat": gap["gap_lat"], "lng": gap["gap_lon"]},
                "description": f"High-risk blind spot identified between {gap['cam1']} and {gap['cam2']} ({int(gap['dist_m'])}m gap)",
                "severity": "critical" if gap['dist_m'] > 1500 else "high",
                "nearest_camera_distance_km": round((gap['dist_m'] / 2.0) / 1000.0, 2)
            })
            
            recommended_deployments.append({
                "lat": gap["gap_lat"],
                "lng": gap["gap_lon"],
                "reason": f"Close {int(gap['dist_m'])}m gap"
            })
            
        if not coverage_gaps:
            coverage_gaps = [{
                "gap_id": "GAP-MOCK",
                "location": {"lat": 23.0300, "lng": 72.5800},
                "description": "Mock blind spot (no sparse pairs found in DB)",
                "severity": "medium",
                "nearest_camera_distance_km": 1.2
            }]
            recommended_deployments = [{
                "lat": 23.0300, "lng": 72.5800,
                "reason": "Close mock gap"
            }]

        return {
            "total_covered_area_km2": round(total_covered_area_km2, 2),
            "coverage_gaps": coverage_gaps,
            "recommended_deployments": recommended_deployments
        }
    except Exception as e:
        logger.error(f"Error in gap analysis: {e}")
        raise HTTPException(status_code=500, detail="Gap analysis computation failed")
