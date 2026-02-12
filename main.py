import io
import os
import re
import zipfile
import pandas as pd

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject, BooleanObject

# -----------------------
# CONFIG
# -----------------------
# Use Render env var APP_PASSWORD if set, otherwise fallback:
PASSWORD = os.getenv("APP_PASSWORD", "InfiniteAccountingServicesInc")

TEMPLATE_NAME = "1099 NEC FORM.pdf"
OUTPUT_ZIP_NAME = "1099_output.zip"
CONTRACTOR_FOLDER = "Contractor's Copy"

# Replicate values into these copies (all are on the IRS 1099-NEC template)
COPIES = ["CopyA[0]", "Copy1[0]", "CopyB[0]", "Copy2[0]"]

# Excel header -> PDF field name (CopyA field names)
MAP_1099 = {
    "FOR CALENDAR\nYEAR": "topmostSubform[0].CopyA[0].PgHeader[0].CalendarYear[0].f1_1[0]",
    "PAYER’S name, street address, city or town, state or province, country, ZIP\nor foreign postal code, and telephone no.": "topmostSubform[0].CopyA[0].LeftCol[0].f1_2[0]",
    "PAYER’S TIN": "topmostSubform[0].CopyA[0].LeftCol[0].f1_3[0]",
    "RECIPIENT’S TIN": "topmostSubform[0].CopyA[0].LeftCol[0].f1_4[0]",
    "RECIPIENT’S name": "topmostSubform[0].CopyA[0].LeftCol[0].f1_5[0]",
    "Street address (including apt. no.)": "topmostSubform[0].CopyA[0].LeftCol[0].f1_6[0]",
    "City or town, state or province, country,\nand ZIP or foreign postal code": "topmostSubform[0].CopyA[0].LeftCol[0].f1_7[0]",
    "1 Nonemployee\ncompensation": "topmostSubform[0].CopyA[0].RightCol[0].f1_9[0]",
    "6 State/ \nPayer's State No.": "topmostSubform[0].CopyA[0].RightCol[0].Box6_ReadOrder[0].f1_14[0]",
    "7 State\nincome": "topmostSubform[0].CopyA[0].RightCol[0].Box7_ReadOrder[0].f1_16[0]",
}

# These columns should be formatted with commas + 2 decimals
AMOUNT_HEADERS = {
    "1 Nonemployee\ncompensation",
    "7 State\nincome",
}

# Required columns to generate filenames / core output
COL_RECIPIENT = "RECIPIENT’S name"
COL_YEAR = "FOR CALENDAR\nYEAR"

app = FastAPI()


# -----------------------
# UI
# -----------------------
@app.get("/", response_class=HTMLResponse)
def home():
    return f"""
    <html>
    <head>
        <title>1099-NEC Auto-Fill</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            body {{ font-family: Arial, sans-serif; background:#f3f4f6; padding:30px; color:#111827; }}
            .card {{ background:white; padding:28px; max-width:680px; margin:auto; border-radius:12px;
                     box-shadow: 0 8px 28px rgba(0,0,0,0.08); border:1px solid #e5e7eb; }}
            h2 {{ margin: 0 0 14px; }}
            label {{ display:block; margin-top: 14px; font-weight: 600; }}
            input[type=password], input[type=file] {{
                width: 100%; padding: 10px; margin-top: 6px; border-radius: 8px;
                border: 1px solid #d1d5db; background: #f9fafb;
            }}
            button {{
                margin-top: 18px; padding: 12px 16px; background:#2563eb; color:white;
                border:none; border-radius:10px; font-weight:700; cursor:pointer; width: 100%;
            }}
            .tip {{ margin-top: 14px; font-size: 13px; color:#6b7280; line-height: 1.5; }}
            code {{ background:#f3f4f6; padding:2px 6px; border-radius:6px; }}
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
                    • Template must exist on the server as: <code>{TEMPLATE_NAME}</code><br/>
                    • Contractor copies will be inside the ZIP folder: <code>{CONTRACTOR_FOLDER}/</code><br/>
                    • Keep the Excel header row exactly the same (spelling + line breaks) or fields won’t match.
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
    In this IRS template, CopyA uses f1_ fields, other copies use f2_ fields.
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


def write_full_pdf_bytes(template_path: str, row: pd.Series) -> bytes:
    """
    Fill CopyA fields based on Excel headers, replicate into all copies, return full PDF bytes.
    """
    reader = PdfReader(template_path)
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)

    # Force appearances (helps some viewers show values)
    if "/AcroForm" in writer._root_object:
        writer._root_object[NameObject("/AcroForm")].update({NameObject("/NeedAppearances"): BooleanObject(True)})

    fields = writer.get_fields() or {}

    for excel_col, copya_field in MAP_1099.items():
        raw = row.get(excel_col)

        if excel_col in AMOUNT_HEADERS:
            val = normalize_amount(raw)
        else:
            val = "" if is_blank(raw) else str(raw).strip()

        for cp in COPIES:
            fname = sibling_field(copya_field, cp)
            set_field_value(writer, fields, fname, val)

    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def contractor_copy_bytes(full_pdf: bytes) -> bytes:
    """
    Contractor copy = pages 3-6 only (drop first 2 pages).
    """
    reader = PdfReader(io.BytesIO(full_pdf))
    writer = PdfWriter()

    for i in range(2, len(reader.pages)):  # keep pages 3..end
        writer.add_page(reader.pages[i])

    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def row_all_blank(row: pd.Series, cols: List[str]) -> bool:
    return all(is_blank(row.get(c)) for c in cols)


# -----------------------
# GENERATE ENDPOINT
# -----------------------
@app.post("/generate")
async def generate(password: str = Form(...), excel: UploadFile = File(...)):
    if password != PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password")

    template_path = os.path.join(os.getcwd(), TEMPLATE_NAME)
    if not os.path.exists(template_path):
        raise HTTPException(status_code=500, detail=f"Missing template on server: {template_path}")

    # Read Excel
    try:
        excel_bytes = await excel.read()
        df = pd.read_excel(io.BytesIO(excel_bytes), dtype=object)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel: {e}")

    # Required for filenames
    missing_required = [c for c in (COL_RECIPIENT, COL_YEAR) if c not in df.columns]
    if missing_required:
        raise HTTPException(
            status_code=400,
            detail=f"Excel missing required columns: {missing_required}. Make sure header row matches exactly.",
        )

    # Build ZIP in memory
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        count = 0
        cols = list(df.columns)

        for _, row in df.iterrows():
            if row_all_blank(row, cols):
                continue

            recipient = clean_filename(row.get(COL_RECIPIENT))
            year = safe_year_value(row.get(COL_YEAR))

            # Generate full PDF (all pages)
            full_pdf = write_full_pdf_bytes(template_path, row)

            # Generate contractor copy (pages 3-6 only)
            contractor_pdf = contractor_copy_bytes(full_pdf)

            # Filenames
            full_name = f"1099 NEC - {recipient} - {year}.pdf"
            contractor_name = f"1099 NEC - {recipient} - Contractor's Copy - {year}.pdf"

            # Add to ZIP
            z.writestr(full_name, full_pdf)
            z.writestr(f"{CONTRACTOR_FOLDER}/{contractor_name}", contractor_pdf)

            count += 1

        if count == 0:
            raise HTTPException(status_code=400, detail="No usable recipient rows found in Excel.")

    zip_buf.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="{OUTPUT_ZIP_NAME}"'}
    return StreamingResponse(zip_buf, media_type="application/zip", headers=headers)
