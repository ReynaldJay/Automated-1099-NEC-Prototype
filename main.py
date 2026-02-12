import io
import os
import re
import zipfile
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject, BooleanObject

# -----------------------
# CONFIG
# -----------------------
# Use Render Environment Variable APP_PASSWORD if set, otherwise fallback:
PASSWORD = os.getenv("APP_PASSWORD", "InfiniteAccountingServicesInc")

TEMPLATE_NAME = "1099 NEC FORM.pdf"
OUTPUT_ZIP_NAME = "1099_output.zip"
CONTRACTOR_FOLDER = "Contractor's Copy"

# Excel headers (must match your sheet header row)
COL_RECIPIENT = "RECIPIENT’S name"
COL_YEAR = "FOR CALENDAR\nYEAR"
COL_AMOUNT_1 = "1 Nonemployee\ncompensation"

# PDF field mappings (Copy A fields)
PDF_FIELD_YEAR = "topmostSubform[0].CopyA[0].PgHeader[0].CalendarYear[0].f1_1[0]"
PDF_FIELD_RECIPIENT_NAME = "topmostSubform[0].CopyA[0].LeftCol[0].f1_5[0]"
PDF_FIELD_AMOUNT_1 = "topmostSubform[0].CopyA[0].RightCol[0].f1_9[0]"

# All copies we replicate into
COPIES = ["CopyA[0]", "Copy1[0]", "CopyB[0]", "Copy2[0]"]


app = FastAPI()


# -----------------------
# UI
# -----------------------
@app.get("/", response_class=HTMLResponse)
def home():
    return f"""
    <html>
    <head>
        <title>Automated Forms - 1099 NEC</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            body {{
                font-family: Arial, sans-serif;
                background:#f3f4f6;
                padding:30px;
                color:#111827;
            }}
            .card {{
                background:white;
                padding:28px;
                max-width:640px;
                margin:auto;
                border-radius:12px;
                box-shadow: 0 8px 28px rgba(0,0,0,0.08);
                border:1px solid #e5e7eb;
            }}
            h2 {{ margin: 0 0 14px; }}
            label {{ display:block; margin-top: 14px; font-weight: 600; }}
            input[type=password], input[type=file] {{
                width: 100%;
                padding: 10px;
                margin-top: 6px;
                border-radius: 8px;
                border: 1px solid #d1d5db;
                background: #f9fafb;
            }}
            button {{
                margin-top: 18px;
                padding: 12px 16px;
                background:#2563eb;
                color:white;
                border:none;
                border-radius:10px;
                font-weight:700;
                cursor:pointer;
                width: 100%;
            }}
            .tip {{
                margin-top: 14px;
                font-size: 13px;
                color:#6b7280;
                line-height: 1.4;
            }}
        </style>
    </head>
    <body>
        <div class="card">
            <h2>1099-NEC Auto-Fill Portal</h2>
            <form action="/generate" method="post" enctype="multipart/form-data">
                <label>Password</label>
                <input type="password" name="password" required />

                <label>Upload Excel (.xlsx)</label>
                <input type="file" name="excel" accept=".xlsx" required />

                <button type="submit">Generate PDFs (ZIP)</button>

                <div class="tip">
                    • The template PDF must exist on the server as: <b>{TEMPLATE_NAME}</b><br/>
                    • Contractor copies will be placed in: <b>{CONTRACTOR_FOLDER}/</b> inside the ZIP.
                </div>
            </form>
        </div>
    </body>
    </html>
    """


# -----------------------
# HELPERS
# -----------------------
def is_blank(x) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    s = str(x).strip()
    return s == "" or s.lower() == "nan"


def normalize_amount(x) -> str:
    # Always commas + 2 decimals; blank becomes 0.00
    if is_blank(x):
        return "0.00"
    try:
        val = float(str(x).replace(",", "").strip())
        return f"{val:,.2f}"
    except Exception:
        return "0.00"


def clean_filename(s: str) -> str:
    if is_blank(s):
        return "UNKNOWN"
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    # remove invalid filename chars
    s = re.sub(r'[\\/:*?"<>|]+', "", s)
    return s or "UNKNOWN"


def safe_year_value(x) -> str:
    if is_blank(x):
        return "YEAR"
    s = str(x).strip()
    digits = re.sub(r"[^\d]", "", s)
    return digits or s or "YEAR"


def sibling_field(copya_field: str, target_copy: str) -> str:
    """
    Replace CopyA segment with other copy segment.
    In many IRS templates, CopyA uses f1_ fields while other copies use f2_.
    """
    out = copya_field.replace("CopyA[0]", target_copy)
    if target_copy != "CopyA[0]":
        out = out.replace(".f1_", ".f2_")
    return out


def set_field_value(writer: PdfWriter, fields: dict, name: str, value: str):
    if name not in fields:
        return
    ind = fields[name].indirect_reference
    obj = writer.get_object(ind)
    obj.update({NameObject("/V"): TextStringObject(value)})


def write_pdf_bytes(template_path: str, year: str, recipient: str, amount1: str) -> bytes:
    """
    Fill CopyA and replicate to all copies, then return the full 6-page PDF bytes.
    """
    reader = PdfReader(template_path)
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)

    # ensure appearance
    if "/AcroForm" in writer._root_object:
        writer._root_object[NameObject("/AcroForm")].update({NameObject("/NeedAppearances"): BooleanObject(True)})

    fields = writer.get_fields() or {}

    # Fill and replicate fields
    for cp in COPIES:
        set_field_value(writer, fields, sibling_field(PDF_FIELD_YEAR, cp), year)
        set_field_value(writer, fields, sibling_field(PDF_FIELD_RECIPIENT_NAME, cp), recipient)
        set_field_value(writer, fields, sibling_field(PDF_FIELD_AMOUNT_1, cp), amount1)

    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def contractor_copy_bytes(full_pdf_bytes: bytes) -> bytes:
    """
    Contractor copy = pages 3-6 only (i.e., drop first 2 pages).
    """
    reader = PdfReader(io.BytesIO(full_pdf_bytes))
    writer = PdfWriter()

    # keep pages 3-6 => indexes 2..end
    for i in range(2, len(reader.pages)):
        writer.add_page(reader.pages[i])

    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def validate_columns(df: pd.DataFrame, required: List[str]) -> List[str]:
    missing = [c for c in required if c not in df.columns]
    return missing


# -----------------------
# GENERATE ENDPOINT
# -----------------------
@app.post("/generate")
async def generate(password: str = Form(...), excel: UploadFile = File(...)):
    if password != PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password")

    template_path = os.path.join(os.getcwd(), TEMPLATE_NAME)
    if not os.path.exists(template_path):
        raise HTTPException(
            status_code=500,
            detail=f"Template PDF is missing on server. Expected: {template_path}",
        )

    try:
        excel_bytes = await excel.read()
        df = pd.read_excel(io.BytesIO(excel_bytes), dtype=object)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel: {e}")

    # Require at least recipient and year. Amount can be defaulted to 0.00 if missing.
    missing = validate_columns(df, [COL_RECIPIENT, COL_YEAR])
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Excel is missing required columns: {missing}. "
                   f"Make sure header row matches exactly.",
        )

    # Build ZIP in memory (fixes your Render temp-file error)
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        count = 0

        for _, row in df.iterrows():
            # skip blank rows
            if all(is_blank(row.get(c)) for c in df.columns):
                continue

            recipient = clean_filename(row.get(COL_RECIPIENT))
            year = safe_year_value(row.get(COL_YEAR))
            amt1 = normalize_amount(row.get(COL_AMOUNT_1)) if COL_AMOUNT_1 in df.columns else "0.00"

            full_name = f"1099 NEC - {recipient} - {year}.pdf"
            contractor_name = f"1099 NEC - {recipient} - Contractor's Copy - {year}.pdf"

            full_pdf = write_pdf_bytes(template_path, year=year, recipient=recipient, amount1=amt1)
            contractor_pdf = contractor_copy_bytes(full_pdf)

            z.writestr(full_name, full_pdf)
            z.writestr(f"{CONTRACTOR_FOLDER}/{contractor_name}", contractor_pdf)

            count += 1

        if count == 0:
            raise HTTPException(status_code=400, detail="No recipient rows found in Excel.")

    zip_buf.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="{OUTPUT_ZIP_NAME}"'}
    return StreamingResponse(zip_buf, media_type="application/zip", headers=headers)
