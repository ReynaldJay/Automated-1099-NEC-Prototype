import io
import os
import re
import time
import uuid
import zipfile
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject, BooleanObject

# =======================
# CONFIG
# =======================
PASSWORD = os.getenv("APP_PASSWORD", "InfiniteAccountingServicesInc")

TEMPLATE_NAME = "1099 NEC FORM.pdf"
DEFAULT_EXCEL_NAME = "1099 NEC Default Format.xlsx"

OUTPUT_ZIP_NAME = "1099_output.zip"
CONTRACTOR_FOLDER = "Contractor's Copy"

# Progress is per RECIPIENT (1 row = 1 recipient)
# Each recipient produces 2 PDFs (main + contractor) but we count as 1 unit.

COPIES = ["CopyA[0]", "Copy1[0]", "CopyB[0]", "Copy2[0]"]

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

AMOUNT_HEADERS = {
    "1 Nonemployee\ncompensation",
    "7 State\nincome",
}

COL_RECIPIENT = "RECIPIENT’S name"
COL_YEAR = "FOR CALENDAR\nYEAR"

# Job retention (avoid memory leaks on free tier)
JOB_TTL_SECONDS = 60 * 30  # keep finished jobs for 30 minutes


# =======================
# JOB STATE
# =======================
@dataclass
class JobState:
    created_at: float = field(default_factory=lambda: time.time())
    total_recipients: int = 0
    done_recipients: int = 0
    status: str = "Queued"
    finished: bool = False
    error: Optional[str] = None
    zip_bytes: Optional[bytes] = None


JOBS: Dict[str, JobState] = {}
JOBS_LOCK = threading.Lock()


def cleanup_jobs():
    """Drop old jobs to keep memory sane."""
    now = time.time()
    with JOBS_LOCK:
        to_delete = []
        for jid, st in JOBS.items():
            if (now - st.created_at) > JOB_TTL_SECONDS:
                to_delete.append(jid)
        for jid in to_delete:
            del JOBS[jid]


# =======================
# FASTAPI APP
# =======================
app = FastAPI()


# =======================
# UI (Single-page)
# =======================
@app.get("/", response_class=HTMLResponse)
def home():
    return f"""
    <html>
    <head>
        <title>1099-NEC Auto-Fill</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            :root {{
              --bg: #f3f4f6;
              --card: #ffffff;
              --text: #111827;
              --muted: #6b7280;
              --border: #e5e7eb;
              --blue: #2563eb;
            }}
            body {{
              font-family: Arial, sans-serif;
              background: var(--bg);
              padding: 30px;
              color: var(--text);
            }}
            .card {{
              background: var(--card);
              padding: 28px;
              max-width: 760px;
              margin: auto;
              border-radius: 14px;
              box-shadow: 0 10px 30px rgba(0,0,0,0.08);
              border: 1px solid var(--border);
            }}
            h2 {{ margin: 0 0 14px; font-size: 26px; }}
            label {{ display:block; margin-top: 14px; font-weight: 700; }}
            input[type=password], input[type=file] {{
              width: 100%;
              padding: 10px;
              margin-top: 8px;
              border-radius: 10px;
              border: 1px solid #d1d5db;
              background: #f9fafb;
            }}
            button {{
              margin-top: 18px;
              padding: 12px 16px;
              background: var(--blue);
              color: white;
              border: none;
              border-radius: 12px;
              font-weight: 800;
              cursor: pointer;
              width: 100%;
              font-size: 15px;
            }}
            button:disabled {{
              opacity: 0.6;
              cursor: not-allowed;
            }}
            .tip {{
              margin-top: 14px;
              font-size: 13px;
              color: var(--muted);
              line-height: 1.55;
            }}
            code {{
              background: #f3f4f6;
              padding: 2px 6px;
              border-radius: 8px;
            }}
            a.link {{
              color: var(--blue);
              font-weight: 800;
              text-decoration: none;
            }}
            a.link:hover {{ text-decoration: underline; }}

            /* Overlay Loader */
            .overlay {{
              position: fixed;
              inset: 0;
              background: rgba(17, 24, 39, 0.55);
              display: none;
              align-items: center;
              justify-content: center;
              padding: 18px;
              z-index: 9999;
              backdrop-filter: blur(3px);
            }}
            .modal {{
              width: 520px;
              max-width: 100%;
              background: #0b1220;
              color: #e5e7eb;
              border-radius: 16px;
              padding: 18px 18px 16px;
              box-shadow: 0 20px 60px rgba(0,0,0,0.35);
              border: 1px solid rgba(255,255,255,0.08);
            }}
            .modal h3 {{
              margin: 0 0 6px;
              font-size: 18px;
              letter-spacing: 0.2px;
            }}
            .modal p {{
              margin: 0 0 12px;
              color: rgba(229,231,235,0.85);
              font-size: 13px;
            }}
            .row {{
              display: flex;
              justify-content: space-between;
              align-items: center;
              margin: 10px 0 8px;
              gap: 10px;
              font-size: 13px;
              color: rgba(229,231,235,0.92);
            }}
            .bar {{
              width: 100%;
              height: 12px;
              background: rgba(255,255,255,0.10);
              border-radius: 999px;
              overflow: hidden;
            }}
            .bar > div {{
              height: 100%;
              width: 0%;
              background: linear-gradient(90deg, #2563eb, #60a5fa);
              border-radius: 999px;
              transition: width 0.25s ease;
            }}
            .spinner {{
              width: 18px;
              height: 18px;
              border-radius: 50%;
              border: 3px solid rgba(255,255,255,0.2);
              border-top-color: rgba(255,255,255,0.9);
              animation: spin 0.8s linear infinite;
            }}
            @keyframes spin {{
              to {{ transform: rotate(360deg); }}
            }}
            .actions {{
              display: flex;
              gap: 10px;
              margin-top: 14px;
            }}
            .btn {{
              flex: 1;
              padding: 10px 12px;
              border-radius: 12px;
              font-weight: 800;
              border: 1px solid rgba(255,255,255,0.14);
              background: rgba(255,255,255,0.08);
              color: #e5e7eb;
              cursor: pointer;
            }}
            .btn.primary {{
              background: linear-gradient(90deg, #2563eb, #3b82f6);
              border: none;
            }}
            .btn:disabled {{
              opacity: 0.5;
              cursor: not-allowed;
            }}
            .error {{
              margin-top: 10px;
              color: #fecaca;
              font-size: 13px;
              white-space: pre-wrap;
            }}
        </style>
    </head>
    <body>
        <div class="card">
            <h2>1099-NEC Auto-Fill Portal</h2>

            <label>Password</label>
            <input id="pw" type="password" placeholder="Enter password" />

            <label>
                Upload Excel (.xlsx) or download our Default Excel Format
                <a class="link" href="/download-template">HERE</a>
            </label>
            <input id="excel" type="file" accept=".xlsx" />

            <button id="startBtn" onclick="startJob()">Generate PDFs (ZIP)</button>

            <div class="tip">
                • Template must exist as: <code>{TEMPLATE_NAME}</code><br/>
                • Contractor copies will be inside ZIP folder: <code>{CONTRACTOR_FOLDER}/</code><br/>
                • Keep the Excel header row exactly the same (including line breaks).
            </div>
        </div>

        <!-- Overlay -->
        <div id="overlay" class="overlay">
          <div class="modal">
            <div style="display:flex; justify-content:space-between; align-items:center; gap:12px;">
              <div>
                <h3 id="ovTitle">Generating PDFs</h3>
                <p id="ovSubtitle">Please keep this tab open.</p>
              </div>
              <div class="spinner" id="spinner"></div>
            </div>

            <div class="row">
              <div id="ovCount">Recipients: 0 / 0</div>
              <div id="ovPercent">0%</div>
            </div>
            <div class="bar"><div id="ovBar"></div></div>

            <div class="row" style="margin-top:10px;">
              <div id="ovStatus">Starting…</div>
            </div>

            <div class="actions">
              <button id="downloadBtn" class="btn primary" onclick="downloadZip()" disabled>Download ZIP</button>
              <button class="btn" onclick="closeOverlay()">Close</button>
            </div>

            <div id="ovError" class="error" style="display:none;"></div>
          </div>
        </div>

        <script>
          let jobId = null;
          let pollTimer = null;

          function showOverlay() {{
            document.getElementById("overlay").style.display = "flex";
            document.getElementById("ovError").style.display = "none";
            document.getElementById("downloadBtn").disabled = true;
            document.getElementById("spinner").style.display = "block";
            document.getElementById("ovBar").style.width = "0%";
            document.getElementById("ovPercent").innerText = "0%";
            document.getElementById("ovCount").innerText = "Recipients: 0 / 0";
            document.getElementById("ovStatus").innerText = "Starting…";
          }}

          function showError(msg) {{
            const el = document.getElementById("ovError");
            el.style.display = "block";
            el.textContent = msg;
            document.getElementById("spinner").style.display = "none";
          }}

          function closeOverlay() {{
            if (pollTimer) {{
              clearInterval(pollTimer);
              pollTimer = null;
            }}
            document.getElementById("overlay").style.display = "none";
          }}

          async function startJob() {{
            const pw = document.getElementById("pw").value;
            const excel = document.getElementById("excel").files[0];

            if (!pw) {{
              alert("Please enter the password.");
              return;
            }}
            if (!excel) {{
              alert("Please choose an Excel (.xlsx) file.");
              return;
            }}

            const btn = document.getElementById("startBtn");
            btn.disabled = true;
            btn.innerText = "Uploading…";

            showOverlay();

            try {{
              const fd = new FormData();
              fd.append("password", pw);
              fd.append("excel", excel);

              const res = await fetch("/start", {{
                method: "POST",
                body: fd
              }});

              const data = await res.json();
              if (!res.ok) {{
                showError(data.detail || "Failed to start job.");
                btn.disabled = false;
                btn.innerText = "Generate PDFs (ZIP)";
                return;
              }}

              jobId = data.job_id;
              document.getElementById("ovStatus").innerText = "Processing…";

              pollTimer = setInterval(pollProgress, 800);
              await pollProgress();

              btn.disabled = false;
              btn.innerText = "Generate PDFs (ZIP)";

            }} catch (e) {{
              showError(String(e));
              btn.disabled = false;
              btn.innerText = "Generate PDFs (ZIP)";
            }}
          }}

          async function pollProgress() {{
            if (!jobId) return;
            try {{
              const res = await fetch(`/progress/${{jobId}}`);
              const data = await res.json();

              if (!res.ok) {{
                showError(data.detail || "Progress error.");
                clearInterval(pollTimer);
                pollTimer = null;
                return;
              }}

              const done = data.done_recipients || 0;
              const total = data.total_recipients || 0;
              const pct = data.percent || 0;

              document.getElementById("ovCount").innerText = `Recipients: ${{done}} / ${{total}}`;
              document.getElementById("ovPercent").innerText = `${{pct}}%`;
              document.getElementById("ovBar").style.width = `${{pct}}%`;
              document.getElementById("ovStatus").innerText = data.status || "Processing…";

              if (data.error) {{
                showError(data.error);
                clearInterval(pollTimer);
                pollTimer = null;
                return;
              }}

              if (data.finished) {{
                document.getElementById("ovTitle").innerText = "Completed";
                document.getElementById("ovSubtitle").innerText = "Your ZIP file is ready.";
                document.getElementById("downloadBtn").disabled = false;
                document.getElementById("spinner").style.display = "none";

                clearInterval(pollTimer);
                pollTimer = null;
              }}

            }} catch (e) {{
              showError(String(e));
              clearInterval(pollTimer);
              pollTimer = null;
            }}
          }}

          function downloadZip() {{
            if (!jobId) return;
            window.location.href = `/download/${{jobId}}`;
          }}
        </script>
    </body>
    </html>
    """


# =======================
# DOWNLOAD DEFAULT EXCEL
# =======================
@app.get("/download-template")
def download_template():
    path = os.path.join(os.getcwd(), DEFAULT_EXCEL_NAME)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Default Excel format not found on server.")

    return StreamingResponse(
        open(path, "rb"),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{DEFAULT_EXCEL_NAME}"'},
    )


# =======================
# HELPERS
# =======================
def is_blank(x) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    s = str(x).strip()
    return s == "" or s.lower() == "nan"


def normalize_amount(x) -> str:
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


def row_all_blank(row: pd.Series, cols: list) -> bool:
    return all(is_blank(row.get(c)) for c in cols)


def write_full_pdf_bytes(template_path: str, row: pd.Series) -> bytes:
    reader = PdfReader(template_path)
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)

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
    # Keep AcroForm + field values by cloning entire doc then removing pages.
    reader = PdfReader(io.BytesIO(full_pdf))
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)

    if "/AcroForm" in writer._root_object:
        writer._root_object[NameObject("/AcroForm")].update({NameObject("/NeedAppearances"): BooleanObject(True)})

    if len(writer.pages) >= 2:
        writer.remove_page(1)
        writer.remove_page(0)

    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


# =======================
# JOB WORKER
# =======================
def run_job(job_id: str, excel_bytes: bytes):
    try:
        cleanup_jobs()

        template_path = os.path.join(os.getcwd(), TEMPLATE_NAME)
        if not os.path.exists(template_path):
            raise RuntimeError(f"Missing template on server: {template_path}")

        # Read Excel
        df = pd.read_excel(io.BytesIO(excel_bytes), dtype=object)

        missing_required = [c for c in (COL_RECIPIENT, COL_YEAR) if c not in df.columns]
        if missing_required:
            raise RuntimeError(f"Excel missing required columns: {missing_required}. Make sure headers match exactly.")

        rows = []
        cols = list(df.columns)
        for _, row in df.iterrows():
            if row_all_blank(row, cols):
                continue
            rows.append(row)

        if not rows:
            raise RuntimeError("No usable recipient rows found in Excel.")

        with JOBS_LOCK:
            st = JOBS[job_id]
            st.total_recipients = len(rows)
            st.done_recipients = 0
            st.status = "Generating…"

        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for idx, row in enumerate(rows, start=1):
                recipient = clean_filename(row.get(COL_RECIPIENT))
                year = safe_year_value(row.get(COL_YEAR))

                with JOBS_LOCK:
                    JOBS[job_id].status = f"Filling: {recipient} ({idx}/{len(rows)})"

                full_pdf = write_full_pdf_bytes(template_path, row)
                contractor_pdf = contractor_copy_bytes(full_pdf)

                full_name = f"1099 NEC - {recipient} - {year}.pdf"
                contractor_name = f"1099 NEC - {recipient} - Contractor's Copy - {year}.pdf"

                z.writestr(full_name, full_pdf)
                z.writestr(f"{CONTRACTOR_FOLDER}/{contractor_name}", contractor_pdf)

                with JOBS_LOCK:
                    JOBS[job_id].done_recipients = idx

        zip_bytes = zip_buf.getvalue()
        with JOBS_LOCK:
            st = JOBS[job_id]
            st.zip_bytes = zip_bytes
            st.status = "Completed"
            st.finished = True

    except Exception as e:
        with JOBS_LOCK:
            st = JOBS.get(job_id)
            if st:
                st.error = str(e)
                st.status = "Error"
                st.finished = True


# =======================
# API ENDPOINTS (Job flow)
# =======================
@app.post("/start")
async def start(password: str = Form(...), excel: UploadFile = File(...)):
    if password != PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password")

    excel_bytes = await excel.read()
    if not excel_bytes:
        raise HTTPException(status_code=400, detail="Excel file upload was empty.")

    job_id = uuid.uuid4().hex

    with JOBS_LOCK:
        JOBS[job_id] = JobState(status="Queued")

    t = threading.Thread(target=run_job, args=(job_id, excel_bytes), daemon=True)
    t.start()

    return JSONResponse({"job_id": job_id})


@app.get("/progress/{job_id}")
def progress(job_id: str):
    cleanup_jobs()
    with JOBS_LOCK:
        if job_id not in JOBS:
            raise HTTPException(status_code=404, detail="Job not found (expired). Please start again.")
        st = JOBS[job_id]

        total = st.total_recipients or 0
        done = st.done_recipients or 0
        percent = int((done / total) * 100) if total else 0

        return {
            "job_id": job_id,
            "total_recipients": total,
            "done_recipients": done,
            "percent": percent,
            "status": st.status,
            "finished": st.finished,
            "error": st.error,
        }


@app.get("/download/{job_id}")
def download(job_id: str):
    cleanup_jobs()
    with JOBS_LOCK:
        if job_id not in JOBS:
            raise HTTPException(status_code=404, detail="Job not found (expired). Please start again.")
        st = JOBS[job_id]

        if not st.finished:
            raise HTTPException(status_code=409, detail="Job still running. Please wait.")
        if st.error:
            raise HTTPException(status_code=400, detail=f"Job failed: {st.error}")
        if not st.zip_bytes:
            raise HTTPException(status_code=500, detail="ZIP not available.")

        zip_bytes = st.zip_bytes

    headers = {"Content-Disposition": f'attachment; filename="{OUTPUT_ZIP_NAME}"'}
    return StreamingResponse(io.BytesIO(zip_bytes), media_type="application/zip", headers=headers)
