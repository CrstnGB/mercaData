from fastapi import FastAPI, UploadFile, File
from processing import process_invoice
import shutil
import os

app = FastAPI()

@app.post("/upload-pdf/")
async def upload_pdf(file: UploadFile = File(...)):
    os.makedirs("temp_files", exist_ok=True)
    temp_path = f"temp_files/{file.filename}"

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = process_invoice(temp_path)
    os.remove(temp_path)

    return result

