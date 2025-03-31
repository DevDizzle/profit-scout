import os
from fastapi import APIRouter, HTTPException
from app.services.gemini_service import analyze_yahoo_data
from app.utils.logger import logger
from google.cloud import storage

router = APIRouter(prefix="/quantative")

# Initialize Google Cloud Storage client
storage_client = storage.Client()
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "aialchemy_bucket")
bucket = storage_client.bucket(GCS_BUCKET_NAME)

def get_yahoo_csv_content(ticker: str) -> str:
    blob_name = f"Yahoo Finance/{ticker}.csv"
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    return blob.download_as_text()

@router.get("/analyze_stock/{ticker}")
async def analyze_stock_api(ticker: str):
    logger.info(f"📡 Received request to analyze Yahoo Finance data for stock: {ticker}")
    csv_content = get_yahoo_csv_content(ticker)
    if not csv_content:
        logger.warning(f"❌ Yahoo Finance CSV not found for {ticker}")
        raise HTTPException(status_code=404, detail="Yahoo Finance data not found for the given ticker")
    
    analysis = analyze_yahoo_data(ticker, csv_content)
    logger.info(f"✅ Quantitative analysis completed for {ticker}")
    return {"ticker": ticker, "quantitative_analysis": analysis}
