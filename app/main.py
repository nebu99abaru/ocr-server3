from fastapi import FastAPI, UploadFile, File
from pathlib import Path
import shutil
import uuid
from tasks import ocr_pdf
from fastapi.responses import JSONResponse

UPLOAD_DIR = Path("/app/uploads")
RESULT_DIR = Path("/results")
UPLOAD_DIR.mkdir(exist_ok=True)
RESULT_DIR.mkdir(exist_ok=True)

app = FastAPI()

jobs = {}

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())
    file_path = UPLOAD_DIR / f"{job_id}_{file.filename}"

    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    task = ocr_pdf.delay(str(file_path), str(RESULT_DIR / f"{job_id}.txt"), job_id)
    jobs[job_id] = {"status": "processing", "task_id": task.id}
    return {"job_id": job_id, "task_id": task.id}

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in jobs:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    return jobs[job_id]

@app.get("/result/{job_id}")
async def get_result(job_id: str):
    result_file = RESULT_DIR / f"{job_id}.txt"
    if not result_file.exists():
        return JSONResponse(status_code=404, content={"error": "Result not available"})
    with open(result_file, "r", encoding="utf-8") as f:
        return {"text": f.read()}