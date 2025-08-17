from celery import Celery
from pathlib import Path
import ocrmypdf
from PIL import Image
import uuid
import fitz  # PyMuPDF
import pytesseract

celery = Celery(
    'worker',
    broker='redis://redis:6379/0',
    backend='redis://redis:6379/0'
)

UPLOAD_DIR = Path("/app/uploads")
RESULT_DIR = Path("/results")

def convert_image_to_pdf(image_path: Path) -> Path:
    image = Image.open(image_path)
    pdf_path = image_path.with_suffix('.pdf')
    image.convert("RGB").save(pdf_path, "PDF", resolution=100.0)
    return pdf_path

def extract_ocr_metadata(pdf_path: Path):
    doc = fitz.open(pdf_path)
    page_count = len(doc)
    scanned_pages = 0
    confidence_per_page = []

    for page_num in range(page_count):
        page = doc.load_page(page_num)
        text = page.get_text()
        if not text.strip():
            scanned_pages += 1
        img = page.get_pixmap()
        image_path = pdf_path.parent / f"page_{page_num + 1}.png"
        img.save(image_path)
        try:
            ocr_data = pytesseract.image_to_data(Image.open(image_path), output_type=pytesseract.Output.DICT)
            conf = [int(c) for c in ocr_data["conf"] if c.isdigit()]
            avg_conf = round(sum(conf) / len(conf), 2) if conf else 0
        except Exception:
            avg_conf = 0
        confidence_per_page.append(avg_conf)
        image_path.unlink(missing_ok=True)

    doc.close()
    return {
        "page_count": page_count,
        "scanned_pages": scanned_pages,
        "digital_pages": page_count - scanned_pages,
        "confidence_per_page": confidence_per_page
    }

@celery.task(name="app.tasks.ocr_pdf")
def ocr_pdf(input_path: str, output_path: str, job_id: str):
    input_path = Path(input_path)
    output_path = Path(output_path)

    if input_path.suffix.lower() in [".jpg", ".jpeg", ".png", ".tiff", ".bmp"]:
        input_path = convert_image_to_pdf(input_path)

    metadata = extract_ocr_metadata(input_path)

    try:
        ocrmypdf.ocr(
            input_pdf=input_path,
            output_pdf=output_path.with_suffix(".pdf"),
            force_ocr=True,
            output_type="pdfa",
            skip_text=False,
            jobs=2
        )
    except Exception as e:
        output_path.write_text(f"OCR failed: {str(e)}")
        return

    text_result_path = output_path
    with open(text_result_path, "w", encoding="utf-8") as f_out:
        for page in fitz.open(output_path.with_suffix(".pdf")):
            f_out.write(page.get_text())
            f_out.write("
" + "-" * 40 + "
")

        f_out.write("
[METADATA]
")
        for k, v in metadata.items():
            f_out.write(f"{k}: {v}
")