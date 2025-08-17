from PIL import Image
from pathlib import Path

def convert_image_to_pdf(image_path: Path) -> Path:
    """
    Converts an image file (JPG, PNG, etc.) to a temporary PDF file for OCR.
    Returns the path to the new PDF file.
    """
    output_pdf_path = image_path.with_suffix(".converted.pdf")

    try:
        image = Image.open(image_path).convert("RGB")
        image.save(output_pdf_path)
    except Exception as e:
        raise RuntimeError(f"Failed to convert image to PDF: {e}")

    return output_pdf_path
