# Layer 1 — Edge Ingestion & VMS Federation

## What this is

The middleware adapter that runs on each departmental server. It taps
existing RTSP streams, decodes only what's needed, and hands throttled
frames to Layer 2 (AI/metadata extraction) — all locally, on-host.
Nothing here ever sends video off the server.

## Design invariants (why the code looks the way it does)

1. **One OS process per camera** (`multiprocessing`, not threads).
   A crash or hang in one stream can't affect any other camera or the
   host process. The OS reclaims a dead process's memory for free.
2. **Bounded, drop-oldest queues.** Every camera's output queue has a
   hard `maxsize`. If Layer 2 falls behind, we drop the oldest frame —
   never block, never grow unbounded. This is the single guarantee
   that prevents an edge adapter from OOM-killing a legacy server.
3. **Decode-time throttling.** We use OpenCV's `grab()`/`retrieve()`
   split to only fully decode frames we're going to keep (5fps target,
   not the camera's native 15-30fps). Cheaper on CPU than decoding
   everything and discarding after.
4. **Two-tier failure detection.** A dead process is caught by
   `is_alive()`. A *frozen but still "connected"* stream — a real and
   common RTSP failure mode — is caught separately by the watchdog
   comparing `last_frame_ts` against `stall_timeout_seconds`.
5. **Config-enforced resource ceiling.** `max_concurrent_streams` in
   the YAML is a hard cap the orchestrator will not exceed, regardless
   of how many cameras are listed. Protects unknown/legacy hardware.

## Running the demo (no real cameras needed)

```bash
pip install -r requirements.txt
cd src
python3 orchestrator.py ../config/demo_cameras.yaml
```

This uses `mock://` stream URLs that generate synthetic frames and
periodically simulate a connection drop, so you can watch — and show
judges — the full lifecycle: connect → stream → throttled handoff to
"Layer 2" → simulated failure → backoff → reconnect, with other
cameras completely unaffected the whole time.

Stop with `Ctrl+C` (SIGINT/SIGTERM are handled for clean shutdown).

## Switching to real cameras

Edit `config/cameras.yaml` (the real, non-demo config):

- Set `rtsp_url` to each camera's actual RTSP stream (from the
  existing VMS/NVR — ask the department's IT admin for these; usually
  `rtsp://<ip>:554/<stream-path>`, sometimes needing embedded
  credentials `rtsp://user:pass@<ip>:554/...`).
- Set `latitude`/`longitude` — this is the ONLY per-camera data,
  besides `id` and `department_id`, that should ever be synced to the
  central PostGIS registry.
- Tune `max_concurrent_streams` down if the host is old/underpowered.
  Start conservative (2-4) and raise only after confirming headroom
  with `top`/`htop` on the department server during a soak test.

Then run against the real config instead of the demo one:
```bash
python3 orchestrator.py ../config/cameras.yaml
```

## What still needs building (next steps for this layer)

- ONVIF auto-discovery (WS-Discovery) so cameras don't have to be
  hand-entered per department — nice-to-have for the demo, necessary
  before real deployment across 26 departments.
- A lightweight heartbeat publisher that pushes `status_summary()`
  (camera up/down/reconnecting — never video) up to Kafka, feeding
  the PostGIS `camera_registry.status` column from Layer 3.
- Per-camera memory/CPU cgroup limits at the OS level as a second
  belt-and-suspenders layer under the in-app queue bounding.

---

## Layer 2 — AI & Metadata Extraction

Consumes the frames Layer 1 is already producing, runs vehicle
detection + plate OCR, and emits structured JSON — currently to
stdout and `/snapshots/` on disk, per this stage's scope (Layer 3
Kafka routing comes later).

### Design decisions and why

| Decision | Reasoning |
|---|---|
| **N AI worker processes share M camera queues** (not 1:1 per camera) | YOLO (~6MB) + EasyOCR (~65MB+ of recognition models) get loaded into memory once per worker process. Loading them once per camera would multiply that footprint by camera count — directly undoing Layer 1's whole point of protecting legacy hardware. `ai_worker_count` is a throughput/memory knob independent of how many cameras exist. |
| **Round-robin, non-blocking queue polling** | A quiet camera never starves a busy one. Layer 1 is never blocked by slow inference — it already drop-oldests independently (see Layer 1's design), this layer just can't assume every frame produced gets seen. |
| **No separate plate-detector model** | Dedicated ANPR weights found online vary wildly in quality/license/provenance. Instead: YOLOv8n (official, COCO-pretrained, verified working — see Verification below) localizes the *vehicle*; EasyOCR's own text detector finds text inside that crop, filtered by a plate-shaped regex. One fewer flaky external dependency, and it holds up fine at the roughly-frontal angles typical of junction cameras. |
| **Regex requires ≥1 digit** | An early test caught a real false-positive: pure-letter OCR reads of street signs ("EXIT", "NO PARKING") pass a naive alphanumeric-length check just as easily as a real plate. Requiring at least one digit filters these out while staying general enough for plates from any Indian state. |
| **Positional glyph-confusion correction** | A synthetic test plate `GJ01AB1234` came back from EasyOCR as `GJOIAB1234` — a real, common OCR failure (0→O, 1→I, and similar look-alikes). Because the standard 10-character Indian plate layout has known letter/digit positions (`LLDDLLDDDD`), wrong-type characters at each position are corrected deterministically rather than just trusting raw OCR output. Verified to fix the observed bug without touching already-correct reads or non-10-character plates. |
| **Per-camera, per-plate dedup with a cooldown window** | A vehicle stopped at a signal gets re-detected on every frame. Without dedup that's a duplicate event roughly every 200ms feeding into Kafka in Layer 3 for zero new information. |
| **Snapshots resized before saving** | Enforces "low-res snapshot," not just as a description — `snapshot_max_width` actually downsamples before writing to disk. |

### Verification performed (not just "should work")

This was tested against real models and a real detection, not just
checked for syntax:

1. **Real YOLOv8n weights downloaded and run** against an actual
   photograph (official Ultralytics sample image) — correctly
   detected a bus at 0.873 confidence with a sensible bounding box.
2. **Plate regex unit-tested** against a battery of real and junk
   strings — confirmed it accepts real plate shapes and rejects
   sign-like text (`EXIT`, `NO PARKING`, `SCHOOL ZONE`).
3. **The 0/O, 1/I OCR confusion bug was caught, not assumed away** —
   EasyOCR misread a synthetic `GJ01AB1234` plate as `GJOIAB1234`.
   The positional-correction fix was added specifically because of
   this, then re-verified to actually resolve it.
4. **Full multiprocessing pipeline run end-to-end**: a real vehicle
   photo (with a plate composited onto it) was streamed through
   Layer 1's mock camera source, picked up by a real Layer 2 AI
   worker process, detected, OCR'd, corrected, deduplicated, and
   written out as both a console JSON payload and a saved snapshot
   file — confirmed by opening the actual saved image afterward.

### Known limitations, stated plainly

- The plate-format correction assumes the common 10-character
  `LLDDLLDDDD` layout. Other valid formats (different RTO-code
  digit counts, BH-series plates, etc.) are left uncorrected rather
  than guessed at — better to under-correct than to silently mangle
  a format the positional assumption doesn't fit.
- OCR accuracy on real, unstaged traffic footage (low light, oblique
  angles, motion blur, dirty plates) has NOT been tested — only a
  clean, well-lit, frontal composite. Expect materially lower
  accuracy on real deployment footage; this is a real limitation to
  budget testing time for.
- EasyOCR's recognition models auto-download (~65MB) from GitHub
  releases on first run of any AI worker — this happens once per
  host and is cached afterward, but the first `orchestrator.py` run
  on a fresh department server will be slower to reach "ready."

### Running it

Same entry point as Layer 1 — Layer 2 starts automatically if a
`layer2:` block exists in the config:

```bash
pip install -r requirements.txt
cd src
python3 orchestrator.py ../config/demo_cameras.yaml
```

Watch for `Layer2 Status: {'ai-worker-0': 'ready', ...}` in the logs
once models finish loading, then JSON payloads print to stdout as
plates are detected, with matching files appearing under
`../snapshots/<camera_id>/`.

Note: `demo_cameras.yaml`'s mock streams are synthetic patterns (no
real vehicles), so you likely won't see detections running the demo
config as-is — that config exists to prove Layer 1's plumbing. To see
Layer 2 actually detect and extract a plate, point a camera's
`rtsp_url` at a real video file or RTSP feed containing vehicles, or
use the `mock://<w>x<h>?image=<path>` scheme (added for this layer)
to serve a static real photo as the "stream."

## Files

| File | Purpose |
|---|---|
| `src/stream_worker.py` | Per-camera isolated process: connect, throttle-decode, bounded-queue emit, reconnect/backoff. Includes the `mock://` synthetic source for demoing without hardware, plus an optional `?image=<path>` mode to serve a real static photo (used for Layer 2 integration testing). |
| `src/orchestrator.py` | Spawns/supervises Layer 1 camera workers AND Layer 2 AI workers, enforces the resource ceiling, runs the stall watchdog for both layers, reports combined status. |
| `src/ai_worker.py` | Layer 2 entry point: model loading, round-robin queue consumption, vehicle detection, plate OCR + correction, dedup, snapshot saving, JSON payload emission. |
| `src/kafka_publisher.py` | Layer 3 fault-tolerant Kafka producer: async send, SQLite spill file for offline buffering, automatic drain on reconnect. |
| `src/consumer.py` | Layer 4 Kafka-to-PostGIS consumer: subscribes to `traffic-anpr-alerts` and `camera-heartbeats`, idempotent inserts, camera registry upserts. |
| `src/api.py` | Layer 4 FastAPI dashboard backend: REST endpoints for cameras/alerts/stats, WebSocket real-time alert push, static file serving. |
| `src/static/index.html` | Layer 4 dashboard frontend: dark-themed Leaflet map, live alert feed, plate search, stats cards with glassmorphism. |
| `config/cameras.yaml` | Real per-department camera config template, now including the `layer2:` and `layer3:` blocks. |
| `config/demo_cameras.yaml` | Synthetic config for demoing/testing Layer 1 plumbing without real RTSP cameras. |
| `docker-compose.yml` | Spins up Zookeeper, Kafka (with topic init), and PostGIS for local development. |

---

## Layer 4 — Cloud Consumer, PostGIS & Dashboard

Consumes Kafka streams from the edge, persists them in a spatial
database (PostGIS), and serves a live web dashboard for monitoring
cameras and ANPR alerts.

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Edge (Layers 1–3)                                      │
│  orchestrator → stream_worker → ai_worker → kafka_pub   │
│                                          │              │
│                                   Kafka (localhost:9092) │
└───────────────────────────────────────────│──────────────┘
                                           │
┌───────────────────────────────────────────│──────────────┐
│  Cloud (Layer 4)                         ▼              │
│                                    consumer.py          │
│                                       │                 │
│                                  PostGIS (5432)         │
│                                       │                 │
│                                    api.py → Dashboard   │
│                                            (port 8000)  │
└─────────────────────────────────────────────────────────┘
```

### Design decisions

| Decision | Reasoning |
|---|---|
| **Separate consumer and API processes** | The Kafka consumer writes to the DB continuously; the API reads from it on demand. Either can be restarted independently without disrupting the other. No in-process coupling. |
| **Idempotent inserts (ON CONFLICT DO NOTHING)** | Kafka's at-least-once delivery means duplicates can arrive. The UNIQUE constraint on `(camera_id, plate_number, detected_at)` silently absorbs them. |
| **WebSocket for live alerts, REST for initial load** | The dashboard gets an initial snapshot via `GET /api/alerts`, then switches to the WebSocket for real-time push. No polling overhead, but graceful fallback. |
| **PostGIS geometry column for camera locations** | Enables spatial queries (nearest-camera, bounding-box search) directly in SQL — critical for a map-based dashboard and future spatial analytics. |
| **DB retry-on-startup** | `consumer.py` retries the PostGIS connection up to 10 times with 3s backoff, handling the race condition when `docker-compose up` starts both services simultaneously. |

### Running it

1. **Start infrastructure** (Kafka + PostGIS):
   ```bash
   docker-compose up -d
   ```

2. **Start the Kafka-to-PostGIS consumer** (in a separate terminal):
   ```bash
   cd src
   python consumer.py
   ```

3. **Start the dashboard API** (in another terminal):
   ```bash
   cd src
   uvicorn api:app --host 0.0.0.0 --port 8000 --reload
   ```

4. **Open the dashboard**: http://localhost:8000

5. **Start the edge pipeline** (generates data):
   ```bash
   cd src
   python orchestrator.py ../config/demo_cameras.yaml
   ```

### API endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/cameras` | All cameras with status and location |
| `GET` | `/api/alerts?limit=50&camera_id=...&plate=...` | Recent ANPR alerts with optional filters |
| `GET` | `/api/stats` | Dashboard summary (totals, today's counts) |
| `WS`  | `/ws/alerts` | Real-time alert push (new detections every ~2s) |
| `GET` | `/` | Dashboard frontend |
