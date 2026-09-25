import os
import hashlib
import sqlite3
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
import logging

logger = logging.getLogger("layer4.routes.evidence")
router = APIRouter()

DB_PATH = str(Path(__file__).resolve().parent.parent / "sentinel.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class EvidenceRequest(BaseModel):
    plate_number: str
    start_time: str
    end_time: str
    operator_name: str

def compute_sha256(filepath: str) -> str:
    if not os.path.exists(filepath):
        return "FILE_MISSING"
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

@router.post("/api/evidence/generate")
def generate_evidence(req: EvidenceRequest):
    plate = req.plate_number.strip().upper()
    try:
        conn = get_db()
        cur = conn.cursor()
        query = """
            SELECT a.detected_at, c.camera_name, c.latitude, c.longitude, a.snapshot_path
            FROM anpr_alerts a
            JOIN camera_registry c ON a.camera_id = c.camera_id
            WHERE a.plate_number = ? AND a.detected_at >= ? AND a.detected_at <= ?
            ORDER BY a.detected_at ASC
        """
        cur.execute(query, (plate, req.start_time, req.end_time))
        sightings = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error querying evidence: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")
        
    if not sightings:
        raise HTTPException(status_code=404, detail="No sightings found in this time range.")
        
    pdf_filename = f"Evidence_Dossier_{plate}_{int(datetime.now().timestamp())}.pdf"
    pdf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", pdf_filename)
    
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()
    # Create a local copy of Heading1 to avoid mutating the shared stylesheet
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER
    title_style = ParagraphStyle(
        'TitleCentered',
        parent=styles['Heading1'],
        alignment=TA_CENTER
    )
    
    # Header
    elements.append(Paragraph("State Crime Records Bureau", title_style))
    elements.append(Paragraph("DIGITAL EVIDENCE DOSSIER", title_style))
    elements.append(Spacer(1, 20))
    
    # Summary Table
    summary_data = [
        ["Target Plate", plate],
        ["Query Time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        ["Operator Name", req.operator_name],
        ["Total Sightings", str(len(sightings))]
    ]
    t = Table(summary_data, colWidths=[150, 300])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    elements.append(t)
    elements.append(Spacer(1, 20))
    
    # Sightings Log
    elements.append(Paragraph("Chronological Sighting Log & Integrity Hashes", styles['Heading2']))
    elements.append(Spacer(1, 10))
    
    for s in sightings:
        raw_path = s["snapshot_path"]
        if raw_path and raw_path.startswith("/snapshots/"):
            local_snap_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "snapshots", raw_path.replace("/snapshots/", ""))
        else:
            local_snap_path = raw_path or ""

        file_hash = compute_sha256(local_snap_path)
        
        info = f"<b>Timestamp:</b> {s['detected_at']}<br/>"
        info += f"<b>Camera:</b> {s['camera_name']}<br/>"
        info += f"<b>GPS:</b> {s['latitude']}, {s['longitude']}<br/>"
        info += f"<b>SHA-256 Hash:</b> {file_hash}"
        
        elements.append(Paragraph(info, styles['Normal']))
        elements.append(Spacer(1, 5))
        
        try:
            if os.path.exists(local_snap_path):
                img = Image(local_snap_path, width=200, height=100)
                elements.append(img)
            else:
                elements.append(Paragraph(f"[Image file not found: {local_snap_path}]", styles['Normal']))
        except Exception:
            pass
            
        elements.append(Spacer(1, 15))
    
    # Section 65B Declaration
    elements.append(Spacer(1, 30))
    elements.append(Paragraph("Declaration under Section 65B of the Indian Evidence Act, 1872", styles['Heading3']))
    decl_text = (
        "I hereby certify that the electronic records contained in this dossier were produced by the SENTINEL "
        "computer system during the ordinary course of its lawful activities. The system was operating properly at all "
        "material times, and the accuracy of its contents has not been affected by any operational failure. "
        "The cryptographic hashes provided above uniquely identify the original digital artifacts."
    )
    elements.append(Paragraph(decl_text, styles['Normal']))
    elements.append(Spacer(1, 40))
    elements.append(Paragraph("___________________________", styles['Normal']))
    elements.append(Paragraph(f"Digital Signature / Operator: {req.operator_name}", styles['Normal']))
    
    doc.build(elements)
    
    return FileResponse(pdf_path, filename=pdf_filename, media_type='application/pdf')
