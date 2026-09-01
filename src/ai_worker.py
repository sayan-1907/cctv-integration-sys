"""
ai_worker.py — Layer 2: AI & Metadata Extraction

Consumes throttled frames from Layer 1's per-camera bounded queues,
runs vehicle detection + plate OCR, and emits a structured JSON
metadata payload. Never touches or re-emits the raw frame itself —
only a small, resized snapshot crop — matching the "discard heavy
raw video, keep only lightweight metadata" constraint from the
architecture doc.

Design invariants:
  1. N worker PROCESSES share M camera queues (N << M is the normal
     case). Model weights (YOLO + EasyOCR) load ONCE per worker
     process, never once per camera — this is the dominant memory
     cost, and duplicating it per camera would undo the whole point
     of Layer 1's resource-ceiling design.
  2. Workers poll their assigned queues non-blocking / round-robin.
     A quiet camera never starves a busy one. Layer 1 is NEVER
     blocked by this layer — it already drop-oldests independently
     of consumer speed (see stream_worker.py); this layer just has
     to not assume every frame produced will be seen.
  3. No separately-sourced plate-detector model. We crop the
     YOLO-detected vehicle region and let EasyOCR's own text detector
     find the plate text inside it, filtered by a plate-shaped regex.
     Fewer external model dependencies of unknown provenance, and
     good enough for the roughly-frontal angles typical of junction
     traffic cameras.
  4. Every OCR read is normalized (uppercase, symbols stripped) and
     regex-validated before being treated as a real plate — junk
     reads on non-plate text (shop signs, other markings) never reach
     the emitted payload.
  5. Per-camera, per-plate dedup with a cooldown window. A vehicle
     stopped at a signal gets re-detected every frame; without dedup
     that's a duplicate metadata event every ~200ms feeding Kafka in
     Layer 3 for no new information.
  6. [Layer 3] Each AI worker process owns a FaultTolerantKafkaPublisher
     instance. Payloads are handed off asynchronously — the inference
     loop is never blocked by network I/O to the central broker. If
     Kafka is unreachable, payloads are spilled to a local SQLite
     database and retried automatically once the connection is restored.
"""

import time
import json
import logging
import re
import multiprocessing as mp
from dataclasses import dataclass
from pathlib import Path
from queue import Empty
from typing import Optional, Dict, List, Tuple, Any

import cv2
import numpy as np

logger = logging.getLogger("layer2.ai_worker")

# COCO class ids for vehicle-like objects (YOLOv8 default weights)
VEHICLE_CLASS_IDS = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}

# Accepted plate text after normalization: uppercase alnum, 4-11 chars,
# and MUST contain at least one digit. Deliberately general rather than
# hard-coded to one state's exact format — 26 departments across
# Gujarat may see plates from anywhere in the country. The digit
# requirement is the important part: without it, pure-letter OCR reads
# of street signs and shop lettering ("EXIT", "NOPARKING") pass a
# naive alnum-length check just as easily as a real plate does.
PLATE_PATTERN = re.compile(r"^(?=.*[0-9])[A-Z0-9]{4,11}$")

# Common Indian plate layout: SS DD LL NNNN (2 state letters, 1-2 RTO
# digits, 1-2 series letters, 4 number digits — most frequently a
# 10-character LLDDLLDDDD string, e.g. "GJ01AB1234"). OCR engines
# routinely confuse visually similar glyphs — 0/O, 1/I/L, 5/S, 8/B,
# 2/Z, 6/G — especially at typical traffic-camera resolution and
# angle. Because we know which POSITIONS in this specific format must
# be letters vs digits, we can correct those confusions deterministically
# for the common 10-char case rather than just accepting raw OCR output.
# This is a real, empirically-observed failure mode (see engineering
# notes) — a synthetic "GJ01AB1234" test plate was read back by EasyOCR
# as "GJOIAB1234" (0->O, 1->I) before this correction was added.
_LETTER_TO_DIGIT = {"O": "0", "I": "1", "L": "1", "S": "5", "B": "8", "Z": "2", "G": "6"}
_DIGIT_TO_LETTER = {"0": "O", "1": "I", "5": "S", "8": "B", "2": "Z", "6": "G"}


def _fix_indian_plate_confusions(s: str) -> str:
    """
    Best-effort positional correction for the common 10-char Indian
    plate layout (2 letters, 2 digits, 2 letters, 4 digits). Only
    applied at exactly 10 chars — shorter/longer strings (different
    series-letter counts) are left as OCR produced them rather than
    guessed at, since position assumptions get unreliable off this
    one common shape.
    """
    if len(s) != 10:
        return s
    expected = "LLDDLLDDDD"  # L = must be letter, D = must be digit
    fixed = list(s)
    for i, (ch, kind) in enumerate(zip(s, expected)):
        if kind == "L" and ch in _DIGIT_TO_LETTER:
            fixed[i] = _DIGIT_TO_LETTER[ch]
        elif kind == "D" and ch in _LETTER_TO_DIGIT:
            fixed[i] = _LETTER_TO_DIGIT[ch]
    return "".join(fixed)


@dataclass
class PlateDetection:
    camera_id: str
    timestamp: float
    plate_number: str
    confidence_score: float
    vehicle_bbox: Tuple[int, int, int, int]
    crop: np.ndarray  # never serialized directly — only ever written to disk


class ModelBundle:
    """
    Holds the loaded YOLO model + EasyOCR reader for one worker
    process. Constructed once per process via load_models(), never
    passed across process boundaries (model objects generally aren't
    cheaply picklable, and we don't want to try — one load per worker
    is exactly the design intent).
    """
    def __init__(self, yolo, ocr_reader):
        self.yolo = yolo
        self.ocr_reader = ocr_reader


def load_models(config: dict) -> ModelBundle:
    """
    Heavy imports are deliberately LOCAL to this function, not at
    module level. This module gets imported by orchestrator.py in the
    main process just to reference run_ai_worker as a Process target;
    we don't want torch/ultralytics/easyocr loaded into the main
    orchestrator process's memory for that. They're only ever
    imported inside the child process that actually calls this.
    """
    from ultralytics import YOLO
    import easyocr
    import torch

    device = config.get("device", "cpu")
    if device == "auto":
        device = "cuda" if torch.cuda.is_available() else "cpu"

    logger.info("Loading YOLO model (%s) on device=%s", config.get("yolo_model", "yolov8n.pt"), device)
    yolo = YOLO(config.get("yolo_model", "yolov8n.pt"))

    logger.info("Loading EasyOCR reader (gpu=%s)", device == "cuda")
    ocr_reader = easyocr.Reader(["en"], gpu=(device == "cuda"), verbose=False)

    return ModelBundle(yolo=yolo, ocr_reader=ocr_reader)


def normalize_plate_text(raw: str) -> Optional[str]:
    """
    Strip whitespace/symbols, uppercase, apply positional glyph-confusion
    correction for the common 10-char Indian plate layout, then validate
    shape. Order matters: correct BEFORE the pattern check, since a raw
    OCR read can fail the "at least one digit" requirement purely because
    a digit got misread as its look-alike letter (e.g. "0"->"O").
    """
    cleaned = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    cleaned = _fix_indian_plate_confusions(cleaned)
    if PLATE_PATTERN.match(cleaned):
        return cleaned
    return None


def extract_plate_from_vehicle_crop(
    crop: np.ndarray, ocr_reader, min_conf: float
) -> Optional[Tuple[str, float]]:
    """
    Runs EasyOCR's text detector+recognizer over a vehicle crop and
    returns the highest-confidence plate-shaped text found, or None.
    We deliberately do NOT try to guess a plate sub-region ourselves —
    EasyOCR's own detector already localizes text regions, and a
    vehicle crop is small enough that running it directly is cheap.
    """
    if crop is None or crop.size == 0:
        return None

    try:
        results = ocr_reader.readtext(crop)
    except Exception as exc:
        logger.debug("OCR failed on crop: %s", exc)
        return None

    best: Optional[Tuple[str, float]] = None
    for _bbox, text, prob in results:
        if prob < min_conf:
            continue
        normalized = normalize_plate_text(text)
        if normalized is None:
            continue
        if best is None or prob > best[1]:
            best = (normalized, float(prob))
    return best


def process_frame(envelope, bundle: ModelBundle, config: dict) -> List[PlateDetection]:
    """Runs vehicle detection, then plate OCR per detected vehicle."""
    frame = envelope.frame
    h, w = frame.shape[:2]

    results = bundle.yolo.predict(
        frame,
        conf=config.get("vehicle_confidence_threshold", 0.35),
        classes=list(VEHICLE_CLASS_IDS.keys()),
        verbose=False,
    )[0]

    detections: List[PlateDetection] = []

    for box in results.boxes:
        x1, y1, x2, y2 = (int(v) for v in box.xyxy[0].tolist())
        veh_conf = float(box.conf[0])

        x1, y1 = max(0, x1), max(0, y1)
        x2, y2 = min(w, x2), min(h, y2)
        if x2 <= x1 or y2 <= y1:
            continue

        crop = frame[y1:y2, x1:x2]
        plate_result = extract_plate_from_vehicle_crop(
            crop, bundle.ocr_reader, config.get("ocr_confidence_threshold", 0.4)
        )
        if plate_result is None:
            continue

        plate_text, ocr_conf = plate_result
        combined_conf = round(veh_conf * ocr_conf, 4)

        detections.append(PlateDetection(
            camera_id=envelope.camera_id,
            timestamp=envelope.timestamp,
            plate_number=plate_text,
            confidence_score=combined_conf,
            vehicle_bbox=(x1, y1, x2, y2),
            crop=crop,
        ))

    return detections


def _resize_for_snapshot(crop: np.ndarray, max_width: int) -> np.ndarray:
    """Enforces the 'low-res snapshot' constraint — never save full-res crops."""
    h, w = crop.shape[:2]
    if w <= max_width:
        return crop
    scale = max_width / float(w)
    return cv2.resize(crop, (max_width, int(h * scale)), interpolation=cv2.INTER_AREA)


def save_snapshot(detection: PlateDetection, snapshot_dir: str, max_width: int) -> str:
    cam_dir = Path(snapshot_dir) / detection.camera_id
    cam_dir.mkdir(parents=True, exist_ok=True)

    small = _resize_for_snapshot(detection.crop, max_width)
    fname = f"{int(detection.timestamp * 1000)}_{detection.plate_number}.jpg"
    path = cam_dir / fname
    cv2.imwrite(str(path), small, [cv2.IMWRITE_JPEG_QUALITY, 70])
    return str(path)


def build_payload(detection: PlateDetection, snapshot_path: str) -> dict:
    return {
        "camera_id": detection.camera_id,
        "timestamp": detection.timestamp,
        "plate_number": detection.plate_number,
        "confidence_score": detection.confidence_score,
        "snapshot_filepath": snapshot_path,
    }


def _init_kafka_publisher(worker_id: str, layer3_cfg: dict):
    """
    Creates and starts a FaultTolerantKafkaPublisher for this AI worker
    process. Import is LOCAL so the main orchestrator process never
    loads confluent_kafka (same pattern as the YOLO/EasyOCR imports).

    Returns None if Layer 3 config is missing or confluent-kafka isn't
    installed — the worker gracefully falls back to stdout-only output.
    """
    if not layer3_cfg:
        logger.info("No 'layer3' config — Kafka publishing disabled, payloads go to stdout only")
        return None

    try:
        from kafka_publisher import FaultTolerantKafkaPublisher
    except ImportError:
        logger.warning(
            "kafka_publisher module or confluent-kafka not available — "
            "falling back to stdout-only output. Install with: "
            "pip install confluent-kafka"
        )
        return None

    try:
        publisher = FaultTolerantKafkaPublisher(
            worker_id=worker_id,
            broker_url=layer3_cfg.get("kafka_broker", "localhost:9092"),
            topic=layer3_cfg.get("kafka_topic", "traffic-anpr-alerts"),
            spill_dir=layer3_cfg.get("kafka_spill_dir", "../kafka_spill"),
            max_in_memory=layer3_cfg.get("kafka_queue_size", 500),
            retry_backoff_s=layer3_cfg.get("kafka_retry_backoff_s", 5.0),
        )
        publisher.start()
        return publisher
    except Exception as exc:
        logger.error("Failed to start Kafka publisher: %s — falling back to stdout", exc)
        return None


def run_ai_worker(
    worker_id: str,
    camera_queue_map: Dict[str, "mp.Queue"],
    config: dict,
    shutdown_event: mp.Event,
    ai_heartbeat_dict,   # multiprocessing.Manager().dict()
    layer3_config: Optional[dict] = None,
):
    """
    Entry point run inside its own process. Round-robins across every
    camera queue it's been handed, non-blocking, so no single camera
    (busy or quiet) starves the others — and Layer 1 is never blocked
    regardless of how slow model inference is on this host.
    """
    logging.basicConfig(level=logging.INFO,
                         format=f"%(asctime)s [%(levelname)s] [{worker_id}] %(message)s")

    ai_heartbeat_dict[worker_id] = {"status": "loading_models", "ts": time.time()}
    try:
        bundle = load_models(config)
    except Exception as exc:
        logger.error("Failed to load models: %s — worker exiting. "
                     "Did you run `pip install -r requirements.txt`?", exc)
        ai_heartbeat_dict[worker_id] = {"status": "failed", "ts": time.time(), "error": str(exc)}
        return

    # ── Layer 3: Initialize Kafka publisher ─────────────────────────
    publisher = _init_kafka_publisher(worker_id, layer3_config)

    ai_heartbeat_dict[worker_id] = {"status": "ready", "ts": time.time()}
    logger.info("Model loading complete — watching %d camera queue(s) (kafka=%s)",
                 len(camera_queue_map), "enabled" if publisher else "disabled")

    camera_ids = list(camera_queue_map.keys())
    recent_plates: Dict[str, Dict[str, float]] = {cid: {} for cid in camera_ids}
    dedup_window = config.get("plate_dedup_window_seconds", 10)
    snapshot_dir = config.get("snapshot_dir", "../snapshots")
    snapshot_max_width = config.get("snapshot_max_width", 240)
    poll_interval = config.get("poll_interval_seconds", 0.05)

    idx = 0
    empty_streak = 0

    while not shutdown_event.is_set():
        if not camera_ids:
            shutdown_event.wait(timeout=1.0)
            continue

        camera_id = camera_ids[idx % len(camera_ids)]
        idx += 1
        q = camera_queue_map[camera_id]

        try:
            envelope = q.get_nowait()
        except Empty:
            empty_streak += 1
            if empty_streak >= len(camera_ids):
                shutdown_event.wait(timeout=poll_interval)
                empty_streak = 0
            continue

        empty_streak = 0
        hb_status = {"status": "processing", "camera_id": camera_id, "ts": time.time()}
        if publisher:
            hb_status["kafka_stats"] = publisher.stats.copy()
        ai_heartbeat_dict[worker_id] = hb_status

        try:
            detections = process_frame(envelope, bundle, config)
        except Exception as exc:
            logger.warning("Inference error on frame from %s: %s", camera_id, exc)
            continue

        cache = recent_plates.setdefault(camera_id, {})
        now = time.time()

        for det in detections:
            last_seen = cache.get(det.plate_number)
            if last_seen is not None and (now - last_seen) < dedup_window:
                continue  # same plate, still within cooldown — skip duplicate event
            cache[det.plate_number] = now

            snapshot_path = save_snapshot(det, snapshot_dir, snapshot_max_width)
            payload = build_payload(det, snapshot_path)

            # Always print to stdout (log trail / backward compatibility)
            print(json.dumps(payload))

            # Publish to Kafka if Layer 3 is configured
            if publisher:
                publisher.publish(payload)

    # ── Graceful shutdown ───────────────────────────────────────────
    if publisher:
        logger.info("Stopping Kafka publisher...")
        publisher.stop()

    ai_heartbeat_dict[worker_id] = {"status": "stopped", "ts": time.time()}
    logger.info("Worker shutting down cleanly")
