import os
import io
import zipfile
import tempfile
import pandas as pd

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, TextStringObject, BooleanObject

PASSWORD = "InfiniteAccountingServicesInc"
TEMPLATE_NAME = "1099 NEC FORM.pdf"

app = FastAPI()

# -----------------------
# HTML PAGE
# -----------------------

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
    <head>
        <title>Automated Forms - 1099 NEC</title>
        <style>
            body { font-family: Arial; background:#f3f4f6; padding:40px; }
            .card { background:white; padding:30px; max-width:600px; margin:auto; border-radius:10px; }
            button { padding:10px 20px; background:#2563eb; color:white; border:none; border-radius:6px; }
        </style>
    </head>
    <body>
        <div class="card">
            <h2>1099-NEC Auto-Fill Portal</h2>
            <form action="/generate" method="post" enctype="multipart/form-data">
                <p>Password:</p>
                <input type="password" name="password" required>
                <p>Upload Excel File (.xlsx):</p>
                <input type="file" name="excel" required>
                <br><br>
                <button type="submit">Generate PDFs</button>
            </form>
        </div>
    </body>
    </html>
    """

# -----------------------
# PDF Helpers
# -----------------------

def normalize_amount(x):
    if pd.isna(x) or str(x).strip() == "":
        return "0.00"
    val = float(str(x).replace(",", ""))
    return f"{val:,.2f}"

def clean_filename(x):
    return str(x).replace("/", "").replace("\\", "").strip()

def set_field(writer, fields, name, value):
    if name in fields:
        obj = writer.get_object(fields[name].indirect_reference)
        obj.update({NameObject("/V"): TextStringObject(value)})

# -----------------------
# GENERATION
# -----------------------

@app.post("/generate")
async def generate(password: str = Form(...), excel: UploadFile = File(...)):

    if password != PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password")

    with tempfile.TemporaryDirectory() as tmpdir:

        # Save uploaded excel
        excel_path = os.path.join(tmpdir, "input.xlsx")
        with open(excel_path, "wb") as f:
            f.write(await excel.read())

        df = pd.read_excel(excel_path, dtype=object)

        template_path = TEMPLATE_NAME
        if not os.path.exists(template_path):
            raise HTTPException(status_code=500, detail="Template PDF missing on server")

        zip_path = os.path.join(tmpdir, "1099_output.zip")
        zipf = zipfile.ZipFile(zip_path, "w")

        for _, row in df.iterrows():
            recipient = clean_filename(row["RECIPIENT’S name"])
            year = str(row["FOR CALENDAR\nYEAR"])

            r = PdfReader(template_path)
            w = PdfWriter()
            w.clone_document_from_reader(r)

            w._root_object[NameObject("/AcroForm")].update({NameObject("/NeedAppearances"): BooleanObject(True)})
            fields = w.get_fields()

            set_field(w, fields, "topmostSubform[0].CopyA[0].LeftCol[0].f1_5[0]", recipient)
            set_field(w, fields, "topmostSubform[0].CopyA[0].RightCol[0].f1_9[0]", normalize_amount(row["1 Nonemployee\ncompensation"]))

            output_pdf = os.path.join(tmpdir, f"1099 NEC - {recipient} - {year}.pdf")
            with open(output_pdf, "wb") as f:
                w.write(f)

            zipf.write(output_pdf, os.path.basename(output_pdf))

        zipf.close()

        return FileResponse(zip_path, filename="1099_output.zip")
