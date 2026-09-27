# Sentinel — ANPR Command Center

> **Statewide Automatic Number Plate Recognition (ANPR) platform** for Gujarat's traffic surveillance grid. Live camera monitoring, AI-powered plate extraction, real-time alert streaming, vehicle route reconstruction, forensic evidence generation, cross-camera identity tracking, and GIS coverage gap analysis — all in one unified system.

---

## Architecture Overview

Sentinel is a four-layer hybrid edge-cloud pipeline. Each layer has a strict contract with its neighbors and can be deployed, restarted, or scaled independently.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         EDGE  (runs on department servers)                   │
│                                                                              │
│   ┌──────────────────┐     ┌───────────────────┐     ┌──────────────────┐   │
│   │   Layer 1        │     │   Layer 2          │     │   Layer 3        │   │
│   │  Stream Worker   │────▶│   AI Worker        │────▶│  Kafka Publisher │   │
│   │                  │     │                    │     │                  │   │
│   │ • RTSP decode    │     │ • YOLOv8n vehicle  │     │ • Async produce  │   │
│   │ • 5 fps throttle │     │   detection        │     │ • SQLite spill   │   │
│   │ • Bounded queue  │     │ • EasyOCR / ALPR   │     │   on disconnect  │   │
│   │ • Auto-reconnect │     │ • Plate correction │     │ • Auto-drain on  │   │
│   │ • mock:// source │     │ • Dedup window     │     │   reconnect      │   │
│   │ • Per-process    │     │ • Snapshot crop    │     │                  │   │
│   └──────────────────┘     └───────────────────┘     └────────┬─────────┘   │
│         ▲                         ▲                            │             │
│         └────────────── orchestrator.py supervises ───────────┘             │
│                         (spawns, watchdogs, resource ceiling)                │
└───────────────────────────────────────────────────────────────┬─────────────┘
                                                                │
                                              Kafka  traffic-anpr-alerts
                                              Topics camera-heartbeats
                                                                │
┌───────────────────────────────────────────────────────────────▼─────────────┐
│                         CLOUD  (Layer 4 — central server)                    │
│                                                                              │
│   ┌──────────────────┐        ┌──────────────────────────────────────────┐  │
│   │   consumer.py    │        │   api.py   (FastAPI — port 8000)         │  │
│   │                  │        │                                          │  │
│   │ • Kafka→SQLite   │        │  REST  ·  WebSocket  ·  MJPEG Stream    │  │
│   │ • Idempotent     │──────▶│  On-Demand AI Extraction Engine          │  │
│   │   inserts        │        │  RBAC API-Key Authentication             │  │
│   │ • Watchlist hit  │        │  Cross-Camera Identity Resolution        │  │
│   │   detection      │        │                                          │  │
│   │ • Fuzzy OCR      │        └──────────────┬───────────────────────────┘  │
│   │   matching       │                       │                              │
│   └────────┬─────────┘          ┌────────────▼──────────────────────────┐  │
│            │                    │   index.html  (Dashboard Frontend)     │  │
│            ▼                    │                                        │  │
│       sentinel.db               │  • Live Leaflet camera map             │  │
│      (SQLite / WAL)             │  • Real-time alert feed (WebSocket)    │  │
│                                 │  • On-demand MJPEG stream viewer       │  │
│                                 │  • Vehicle route reconstruction        │  │
│                                 │  • Watchlist management & siren        │  │
│                                 │  • Section 65B evidence dossier (PDF)  │  │
│                                 │  • GIS coverage gap analysis           │  │
│                                 │  • Cross-camera identity merge alerts  │  │
│                                 └────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Features

### Layer 1 — Edge Stream Ingestion

- **One OS process per camera.** A crash or hang in one stream cannot affect any other camera or the host.
- **Bounded, drop-oldest queues.** Every camera queue has a hard `maxsize`. If Layer 2 falls behind, we drop the oldest frame — never block, never OOM.
- **Decode-time throttling.** Uses OpenCV `grab()`/`retrieve()` to only fully decode frames at a 5 fps target (not the camera's native 15–30 fps).
- **Two-tier failure detection.** Dead processes caught by `is_alive()`; *frozen-but-connected* streams caught by a watchdog comparing `last_frame_ts` against `stall_timeout_seconds`.
- **`mock://` synthetic source.** Runs without any real cameras for demos and CI — generates synthetic frames and simulates connection drops.
- **Config-enforced resource ceiling.** `max_concurrent_streams` in YAML is a hard cap the orchestrator will not exceed.

### Layer 2 — AI & Metadata Extraction

- **YOLOv8n vehicle detection** (official Ultralytics weights, COCO-pretrained). Detects cars, motorcycles, buses, trucks.
- **EasyOCR / fast-alpr plate reading** inside the YOLO vehicle crop — no separately-sourced ANPR model needed.
- **Positional glyph-confusion correction.** Deterministically corrects common OCR errors (0↔O, 1↔I, 5↔S, 8↔B, 2↔Z) at known letter/digit positions in the standard 10-character Indian plate format (`LLDDLLDDDD`).
- **Plate regex validation.** Normalized, uppercase, 4–11 chars, must contain ≥1 digit — filters shop signs and non-plate text.
- **Per-camera, per-plate dedup window.** Suppresses repeated detections of the same plate within a configurable cooldown.
- **Snapshot crop saving.** Saves a resized low-res JPEG crop of the vehicle region — never the full frame.
- **N workers, M queues.** YOLO + OCR models load once per worker process, not once per camera.

### Layer 3 — Fault-Tolerant Kafka Publishing

- **Async produce.** The inference loop is never blocked by network I/O to the central broker.
- **SQLite spill file.** If Kafka is unreachable, undelivered payloads spill to a local per-worker SQLite database.
- **Auto-drain on reconnect.** When the broker comes back, spilled payloads are drained in order before new ones are sent.
- **Two topics:** `traffic-anpr-alerts` (plate detections) and `camera-heartbeats` (camera status).

### Layer 4 — Cloud Dashboard & API

#### On-Demand Camera Engine
- Cameras run **idle by default** — no continuous AI inference across all 30+ cameras simultaneously.
- When a user opens a camera in the browser, **YOLOv8 + ALPR activates dynamically** for that specific camera only.
- **Concurrency safety cap** (default: 2 simultaneous extractions). When the cap is hit, the oldest idle extraction is evicted.
- **Auto-reclaim.** Camera worker threads shut down automatically after 5s with no viewers and no active extraction.

#### Live Video Streaming
- **MJPEG proxy stream** — single-pipeline shared decoder per camera, multiple viewers served from one thread.
- **HLS direct CDN stream** — via `hls.js`, connects directly to `cctv.corp8.cloud/{camera_id}/index.m3u8` with low latency.
- **Snapshot mode** (default) — lightweight single-JPEG refresh every 3s, no continuous stream overhead.
- **Tactical standby frame** — professional CCTV-style diagnostic pattern when RTSP feed is offline/unauthorized.
- **Simulated tactical frame** — animated vehicle overlay when RTSP offline and extraction is active (demo mode).
- **RTSP credential injection** — `RTSP_AUTH_EMAIL` / `RTSP_AUTH_PASSWORD` from `.env` are URL-encoded and injected per-camera at startup.

#### Real-Time Alert Feed
- **WebSocket push** (`/ws/alerts`) — new detections broadcast to all connected browser clients within ~1.5s.
- **Alert deduplication** — per-plate, per-camera cooldown window (60s on-demand, 8s Layer 2 pipeline).
- **Confidence tracking** — best confidence per plate is updated in-memory and in SQLite as the vehicle lingers.
- **Sighting count** — how many times a plate has been seen (shown as a badge in the feed).
- **Plate search** — debounced real-time search across the alert feed.
- **Global background extraction** — randomly simulates AI extraction across all cameras to populate the live feed even when no camera is open.

#### RBAC — API Key Authentication
- **SHA-256 hashed API keys** stored in SQLite — raw keys are never stored, only shown once at mint time.
- **Bootstrap admin key** auto-minted on first startup — printed to console.
- **Department-scoped access** — operators see only their department's cameras; admins see all.
- **Role-based:** `admin` | `operator`.
- **Admin key management** — `POST /api/auth/keys` to mint new scoped keys (admin only).
- Frontend stores the API key in `localStorage` and auto-validates on page load.

#### Vehicle Route Reconstruction
- Enter a plate number → reconstructs its **full chronological journey** across the camera grid.
- Calculates **distance** (Haversine), **duration**, and **estimated speed** between each camera hop.
- **Speed anomaly detection** — flags segments where inferred speed exceeds 160 km/h.
- **OSRM road-snapping** — queries `router.project-osrm.org` to snap the route to actual road geometry (GeoJSON polyline).
- Rendered on an interactive **Leaflet map** with numbered sighting markers and a scrollable timeline.

#### Watchlist Management
- Add target vehicles by plate number, reason, and priority (Critical / High / Medium).
- **Fuzzy OCR matching** — catches 1-character OCR confusion errors (e.g., `GJ01AB123B` vs `GJ01AB1238`).
- **In-memory cache** refreshed every 60s from SQLite — zero DB hit per Kafka message.
- **Watchlist siren alert** — a full-screen banner with audio beep fires instantly when a target is detected.
- Soft-delete (deactivation) rather than hard delete — preserves audit trail.

#### Section 65B Evidence Dossier
- Generate a **court-admissible PDF** for any plate within a time range.
- Includes: timestamped sightings log, camera name, GPS coordinates, embedded snapshot images.
- **SHA-256 cryptographic hash** computed for each snapshot file — tamper evidence.
- Auto-includes a **Section 65B Indian Evidence Act declaration** signed by the operator name.
- PDF served as a file download directly from the browser.

#### GIS Coverage Gap Analysis
- Visualizes the camera network's **150m coverage radius** as green circles on a Leaflet map.
- Identifies **blind spots** (camera pairs with 800m–2000m gaps) and marks them as red pulsing markers.
- Suggests **recommended deployment locations** (midpoint of each gap pair) with amber pins.
- Reports: km² covered, number of critical gaps, number of suggested new nodes.

#### Cross-Camera Identity Resolution
- When a plate is **unreadable on one camera** (low light, obstructed angle), the vehicle is logged as an **anonymous track** with its type and color.
- When the same vehicle's plate is **successfully read on another camera**, Sentinel retroactively links all anonymous sightings of that vehicle type+color from the past 5 minutes.
- **Merge banner** — a real-time notification appears in the dashboard with audio chord when a retroactive merge succeeds.
- Anonymous tracks stored in `anonymous_vehicle_tracks` table with `resolved_plate`, `resolved_at`, and `resolved_camera` fields.

---

## Dashboard UI

| Tab | Description |
|-----|-------------|
| **📡 Dashboard** | Live Leaflet camera map + real-time alert feed. Click any camera pin or use the quick-select dropdown to open the stream viewer. |
| **🗺️ Route Tracker** | Vehicle journey reconstruction — enter a plate, get a map + timeline. |
| **🔴 Watchlist** | Target vehicle management — add/remove targets, view active watchlist. |
| **📋 Evidence** | Section 65B PDF dossier generator with SHA-256 integrity hashes. |
| **🛰️ Coverage** | GIS gap analysis — coverage circles, blind spots, suggested deployments. |
| **🔄 Cross-Cam** | Anonymous vehicle tracking and cross-camera identity resolution. |

---

## API Reference

### Core Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/` | — | Serves the dashboard frontend |
| `GET` | `/api/stats` | — | Summary stats (cameras, alerts today, unique plates, total detections, active extractions) |
| `GET` | `/api/cameras` | ✅ Key | All cameras with status, coordinates, last seen |
| `GET` | `/api/alerts` | — | Recent ANPR alerts with optional `?camera_id=`, `?plate=`, `?limit=` filters. Deduplicated per plate. |
| `GET` | `/api/alerts/search` | — | Full-text plate search (partial match) |
| `WS` | `/ws/alerts` | — | Real-time WebSocket alert stream |

### Camera Streaming & Extraction

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/cameras/{id}/snapshot` | Single JPEG snapshot (lightweight, no continuous stream) |
| `GET` | `/api/cameras/{id}/stream` | MJPEG continuous live stream (multipart/x-mixed-replace) |
| `POST` | `/api/cameras/{id}/extract/start` | Activate on-demand AI extraction for this camera |
| `POST` | `/api/cameras/{id}/extract/stop` | Deactivate extraction, free CPU |
| `GET` | `/api/cameras/{id}/extract/status` | Is extraction currently active? |
| `GET` | `/api/cameras/{id}/alerts` | Recent alerts specifically for this camera (merged with in-memory) |
| `GET` | `/api/cameras/extractions` | List all currently active extraction camera IDs |
| `GET` | `/api/streams/active` | Cameras with live viewers or active extraction |

### RBAC

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/api/auth/validate` | ✅ Key | Validate key — returns role and department |
| `POST` | `/api/auth/keys` | ✅ Admin | Mint a new scoped API key |

### Feature APIs

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/vehicles/track` | Route reconstruction for a plate number |
| `GET` | `/api/watchlist` | List active watchlist targets |
| `POST` | `/api/watchlist` | Add / re-activate a target plate |
| `DELETE` | `/api/watchlist/{plate}` | Soft-delete a target plate |
| `POST` | `/api/evidence/generate` | Generate Section 65B PDF dossier |
| `GET` | `/api/cameras/coverage-analysis` | GIS gap analysis (blind spots + recommendations) |
| `GET` | `/api/anonymous-tracks` | Cross-camera anonymous vehicle tracks (`?resolved=true/false`) |
| `POST` | `/api/internal/watchlist-hit` | Internal: consumer → API WebSocket broadcast hook |

---

## Database Schema

```sql
-- One row per physical camera
camera_registry (
    camera_id TEXT PRIMARY KEY,
    department_id TEXT,
    camera_name TEXT,
    latitude REAL, longitude REAL,
    status TEXT,  -- 'idle' | 'streaming' | 'extracting' | 'reconnecting' | 'offline'
    last_seen TEXT,
    registered_at TEXT
)

-- One row per (camera, plate, timestamp) detection event
anpr_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    camera_id TEXT REFERENCES camera_registry,
    plate_number TEXT NOT NULL,
    confidence REAL,
    snapshot_path TEXT,   -- relative URL e.g. /snapshots/cam01/1234567_GJ01AB1234.jpg
    detected_at TEXT NOT NULL,
    ingested_at TEXT,
    vehicle_type TEXT,
    vehicle_color TEXT,
    UNIQUE (camera_id, plate_number, detected_at)
)

-- SHA-256 hashed API keys for RBAC
api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_hash TEXT UNIQUE NOT NULL,
    label TEXT NOT NULL,
    department TEXT NOT NULL DEFAULT 'ALL',
    role TEXT NOT NULL DEFAULT 'operator',  -- 'admin' | 'operator'
    created_at TEXT
)

-- Target vehicles for real-time matching
watchlist_vehicles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plate_number TEXT UNIQUE NOT NULL,
    reason TEXT,
    priority TEXT DEFAULT 'high',  -- 'critical' | 'high' | 'medium'
    added_at DATETIME,
    is_active BOOLEAN DEFAULT 1
)

-- Anonymous vehicle sightings for cross-camera resolution
anonymous_vehicle_tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anon_id TEXT NOT NULL,        -- e.g. 'ANON-cam12-RedSedan-A3F1B2'
    camera_id TEXT NOT NULL,
    vehicle_type TEXT,
    vehicle_color TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    resolved_plate TEXT,          -- filled when retroactively matched
    resolved_at TEXT,
    resolved_camera TEXT
)
```

---

## File Reference

| File | Layer | Purpose |
|------|-------|---------|
| `src/orchestrator.py` | 1+2 | Spawns and supervises all stream workers and AI workers. Enforces resource ceiling, runs stall watchdog for both layers. |
| `src/stream_worker.py` | 1 | Per-camera isolated process: RTSP connect, throttle-decode, bounded-queue emit, backoff/reconnect. Includes `mock://` synthetic source. |
| `src/ai_worker.py` | 2 | Model loading (YOLOv8n + EasyOCR/fast-alpr), round-robin queue consumption, vehicle detection, plate OCR + glyph correction, dedup, snapshot saving, JSON payload emission. |
| `src/kafka_publisher.py` | 3 | Fault-tolerant Kafka producer: async send, SQLite spill on disconnect, auto-drain on reconnect. |
| `src/consumer.py` | 4 | Kafka-to-SQLite consumer: subscribes to `traffic-anpr-alerts` and `camera-heartbeats`, idempotent inserts, camera upserts, watchlist hit detection + fuzzy OCR matching, WebSocket broadcast hook. |
| `src/api.py` | 4 | FastAPI application: all REST/WebSocket endpoints, on-demand extraction engine, MJPEG streaming, RBAC, cross-camera identity resolution, global background mock extraction loop. |
| `src/routes_vehicle.py` | 4 | Route reconstruction API — Haversine distance/speed calculation, OSRM road-snapping. |
| `src/watchlist_api.py` | 4 | Watchlist CRUD API — add, list, soft-delete target plates. |
| `src/evidence_api.py` | 4 | Section 65B PDF dossier generator — SHA-256 snapshot hashing, ReportLab PDF with sightings log and legal declaration. |
| `src/gap_analysis_api.py` | 4 | GIS coverage gap analysis — Haversine-based blind-spot detection, deployment recommendations. |
| `src/static/index.html` | 4 | Single-file dashboard frontend: Leaflet maps, WebSocket alert feed, MJPEG/HLS stream modal, route tracker, watchlist, evidence form, coverage map, cross-camera merge banners. |
| `config/cameras.yaml` | — | Real per-department camera configuration (RTSP URLs, lat/lng, department IDs). |
| `config/demo_cameras.yaml` | — | Synthetic demo config using `mock://` URLs — runs without any hardware. |
| `config/live_cameras.yaml` | — | Live production camera config for the RTSP gateway (`stream.corp8.cloud`). |
| `docker-compose.yml` | — | Spins up Zookeeper, Kafka (with topic initialization), and PostGIS for local development. |
| `.env` / `.env.example` | — | RTSP gateway credentials (`RTSP_AUTH_EMAIL`, `RTSP_AUTH_PASSWORD`). |

---

## Setup & Running

### Prerequisites

```bash
pip install -r requirements.txt
```

Key dependencies:
- `ultralytics` — YOLOv8n vehicle detection
- `easyocr` + `fast-alpr[onnx]` — plate OCR engines
- `fastapi` + `uvicorn` — dashboard API server
- `confluent-kafka` — Kafka producer/consumer
- `reportlab` — PDF evidence dossier generation
- `opencv-python` — RTSP decoding, frame generation

### Option A — Full Demo (No Hardware)

```bash
# Terminal 1: Start the dashboard API
cd files/src
uvicorn api:app --host 0.0.0.0 --port 8000

# Terminal 2: Start the edge pipeline (mock cameras)
cd files/src
python orchestrator.py ../config/demo_cameras.yaml
```

Open **http://localhost:8000** — the bootstrap admin API key is printed to Terminal 1 on first run.

### Option B — With Real Cameras

1. Copy `.env.example` to `.env` and set your RTSP gateway credentials:
   ```
   RTSP_AUTH_EMAIL=your@email.com
   RTSP_AUTH_PASSWORD=yourpassword
   ```

2. Edit `config/live_cameras.yaml` with your camera IDs, RTSP URLs, and GPS coordinates.

3. Start infrastructure (Kafka + PostGIS):
   ```bash
   docker-compose up -d
   ```

4. Start all services:
   ```bash
   # Edge pipeline (on each department server)
   python src/orchestrator.py config/live_cameras.yaml

   # Cloud consumer (central server)
   python src/consumer.py

   # Dashboard API (central server)
   uvicorn src.api:app --host 0.0.0.0 --port 8000
   ```

### API Key Flow

On first startup, the bootstrap admin key is printed to console **once**:
```
============================================================
  [SENTINEL] BOOTSTRAP API KEY (save this — shown only once!)
  Key:   <raw-key>
  Role:  admin (sees ALL departments)
============================================================
```

Enter this key in the dashboard's top-right input and click **Connect**. To mint department-scoped operator keys:

```bash
curl -X POST http://localhost:8000/api/auth/keys \
  -H "X-API-Key: <admin-key>" \
  -H "Content-Type: application/json" \
  -d '{"label": "AHM Traffic Ops", "department": "GJ-AHM-TRAFFIC-01", "role": "operator"}'
```

---

## Design Decisions

### Why SQLite, not PostGIS?

The original architecture targeted PostGIS for spatial queries. For the current deployment model — where the API and consumer run on the same host — SQLite with WAL mode provides:
- Zero separate DB process to manage
- Haversine calculations in Python replace `ST_DistanceSphere`
- `ON CONFLICT DO NOTHING` still gives idempotent inserts
- Easy to migrate to PostgreSQL/PostGIS when the deployment scales

### Why On-Demand Extraction Instead of Always-On?

Running YOLOv8 on 30+ simultaneous RTSP streams causes OOM crashes on the central server. The on-demand model means:
- Idle cameras use ~0% CPU
- Maximum 2 simultaneous AI inference sessions (configurable)
- The user's browser click is the trigger — computation follows attention, not the other way around

### Why a Per-Camera Worker Thread?

Camera sessions run as Python threads rather than subprocesses in Layer 4 because:
- The MJPEG generator is an async generator in the FastAPI event loop
- Thread-based sessions allow the async event loop to yield frames without blocking
- The RLock on each session prevents race conditions between the MJPEG reader, the AI inference caller, and the background idle-reclaim logic

### Why `INSERT OR IGNORE` + UNIQUE constraint?

Kafka's at-least-once delivery means the same plate detection can arrive multiple times. The `UNIQUE(camera_id, plate_number, detected_at)` constraint silently absorbs duplicates without any application-level deduplication overhead.

---

## Known Limitations

- Plate OCR accuracy on real traffic footage (low light, motion blur, oblique angles, dirty plates) has not been benchmarked — only clean, frontal, well-lit frames have been verified.
- The positional glyph-confusion correction assumes the common 10-character `LLDDLLDDDD` Indian plate format. BH-series plates, shorter RTO codes, and non-standard formats are left uncorrected.
- EasyOCR downloads ~65MB of recognition models from GitHub on first run — the first startup on a fresh server is slow.
- The OSRM road-snapping call in route reconstruction is synchronous with a 3s timeout — under load this can add latency.
- The `@app.on_event("startup")` FastAPI lifecycle hook is deprecated in FastAPI 0.93+; migration to `lifespan` is pending.
