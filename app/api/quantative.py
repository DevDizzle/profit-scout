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
    logger.info(f"Attempting to fetch blob '{blob_name}' from bucket '{GCS_BUCKET_NAME}'.")
    blob = bucket.blob(blob_name)
    try:
        if not blob.exists():
            logger.error(f"Blob '{blob_name}' does not exist in bucket '{GCS_BUCKET_NAME}'.")
            return None
        logger.info(f"Blob '{blob_name}' found. Downloading content...")
        content = blob.download_as_text()
        logger.info(f"Successfully downloaded CSV content for ticker: {ticker}. Content length: {len(content)} characters.")
        return content
    except Exception as e:
        logger.exception(f"Error downloading blob '{blob_name}' for ticker '{ticker}': {e}")
        return None

@router.get("/analyze_stock/{ticker}")
async def analyze_stock_api(ticker: str):
    logger.info(f"📡 Received request to analyze Yahoo Finance data for stock: {ticker}")
    csv_content = get_yahoo_csv_content(ticker)
    if not csv_content:
        logger.warning(f"❌ Yahoo Finance CSV not found for ticker: {ticker}")
        raise HTTPException(status_code=404, detail="Yahoo Finance data not found for the given ticker")
    
    logger.info(f"Starting quantitative analysis for ticker: {ticker}")
    analysis = analyze_yahoo_data(ticker, csv_content)
    logger.info(f"✅ Quantitative analysis completed for ticker: {ticker}")
    return {"ticker": ticker, "quantitative_analysis": analysis}
