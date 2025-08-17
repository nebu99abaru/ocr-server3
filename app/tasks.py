from celery import Celery
from pathlib import Path
import shutil
import ocrmypdf
import fitz  # PyMuPDF
import pytesseract
from pdf2image import convert_from_path
from typing import Dict
from utils import convert_image_to_pdf

app = Celery("tasks", broker="redis://redis:6379/0", backend="redis://redis:6379/0")

@app.task(name="app.tasks.ocr_pdf")
def ocr_pdf(input_path: str, output_path: str, job_id: str):
    input_path = Path(input_path)
    output_path = Path(output_path)

    # Convert image to PDF if necessary
    if input_path.suffix.lower() in [".jpg", ".jpeg", ".png", ".tiff", ".bmp"]:
        input_path = convert_image_to_pdf(input_path)

    try:
        # Run OCR and produce a PDF/A file
        ocrmypdf.ocr(
            input_path,
            output_path.with_suffix(".pdf"),
            force_ocr=True,
            output_type="pdfa",
            skip_text=False,
            jobs=2,
        )
    except Exception as e:
        output_path.write_text(f"OCR failed: {str(e)}")
        return

    # === Extract metadata AFTER OCR is complete ===
    metadata = extract_ocr_metadata(output_path.with_suffix(".pdf"))

    # === Write text and metadata ===
    text_result_path = output_path
    with open(text_result_path, "w", encoding="utf-8") as f_out:
        for page in fitz.open(output_path.with_suffix(".pdf")):
            f_out.write("\n" + "-" * 40 + "\n")
            f_out.write(page.get_text())

        f_out.write("\n\n[METADATA]\n")
        for k, v in metadata.items():
            f_out.write(f"{k}: {v}\n")

    # Cleanup: delete original uploads and intermediate PDFs
    try:
        # Delete original image if one was uploaded
        if original_image_path and original_image_path.exists():
            original_image_path.unlink()

        # Delete converted .pdf (intermediate temp file)
        if is_temp_pdf and input_path.exists():
            input_path.unlink()

        # Delete original uploaded PDF (if not converted)
        if not is_temp_pdf and input_path.exists():
            input_path.unlink()

    except Exception as e:
        print(f"Warning: Failed to delete temporary files: {e}")

# === OCR Metadata Extraction ===
# === OCR Metadata Extraction ===
def extract_ocr_metadata(pdf_path: Path) -> Dict:
    metadata = {
        "page_count": 0,
        "scanned_pages": 0,
        "digital_pages": 0,
        "confidence_per_page": [],
    }

    try:
        images = convert_from_path(pdf_path)
        metadata["page_count"] = len(images)

        for image in images:
            tsv = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)

            # Safely extract valid confidence scores
            conf_values = []
            for conf in tsv.get("conf", []):
                try:
                    conf_int = int(conf)
                    if conf_int > 0:
                        conf_values.append(conf_int)
                except (ValueError, TypeError):
                    continue

            avg_conf = round(sum(conf_values) / len(conf_values), 2) if conf_values else 0
            metadata["confidence_per_page"].append(avg_conf)

            # Classify as scanned vs digital: assume scanned if confidence > 0
            if avg_conf > 0:
                metadata["scanned_pages"] += 1
            else:
                metadata["digital_pages"] += 1

    except Exception as e:
        metadata["error"] = str(e)

    return metadata
