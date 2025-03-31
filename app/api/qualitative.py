import os
import tempfile
from fastapi import APIRouter, HTTPException
from app.services.gemini_service import analyze_pdf_content
from app.utils.logger import logger
from google.cloud import storage
import PyPDF2

router = APIRouter(prefix="/qualitative")

# Initialize Google Cloud Storage client
storage_client = storage.Client()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "aialchemy_bucket")
bucket = storage_client.bucket(GCS_BUCKET_NAME)

def download_sec_pdf(ticker: str) -> str:
    """
    Retrieve the SEC filing PDF for a given ticker from GCS.
    Assumes the PDF is stored in the "SEC/" folder and the file name starts with the ticker.
    Returns the local file path of the downloaded PDF.
    """
    blobs = list(bucket.list_blobs(prefix="SEC/"))
    for blob in blobs:
        if blob.name.split("/")[-1].startswith(ticker):
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            blob.download_to_filename(temp_file.name)
            return temp_file.name
    return None

def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Extract text from a PDF file.
    """
    text = ""
    with open(pdf_path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text

@router.get("/analyze_sec/{ticker}")
async def analyze_sec_api(ticker: str):
    logger.info(f"📡 Received request to analyze SEC filing for stock: {ticker}")
    pdf_path = download_sec_pdf(ticker)
    if not pdf_path:
        logger.warning(f"❌ SEC filing PDF not found for {ticker}")
        raise HTTPException(status_code=404, detail="SEC filing PDF not found for the given ticker")
    
    try:
        pdf_text = extract_text_from_pdf(pdf_path)
        analysis = analyze_pdf_content(ticker, pdf_text)
        logger.info(f"✅ SEC filing analysis completed for {ticker}")
        return {"ticker": ticker, "qualitative_analysis": analysis}
    except Exception as e:
        logger.error(f"❌ Error during SEC filing analysis for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Error during analysis: {e}")
    finally:
        if pdf_path and os.path.exists(pdf_path):
            os.remove(pdf_path)
