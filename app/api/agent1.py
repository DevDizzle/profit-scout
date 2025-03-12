from fastapi import APIRouter, HTTPException
from app.models.ratios import FinancialRatios
from app.services.bigquery_service import get_financial_ratios
from app.services.gemini_service import analyze_stock
from app.utils.logger import logger  # Import logger

# Initialize FastAPI Router
router = APIRouter()

### 🔹 Endpoint: Analyze stock using financial ratios & Gemini
@router.get("/analyze_stock/{ticker}")
async def analyze_stock_api(ticker: str):
    """Retrieve financial ratios and analyze the stock using Gemini AI."""
    logger.info(f"📡 Received request to analyze stock: {ticker}")

    # Fetch financial ratios from BigQuery
    ratios_data = get_financial_ratios(ticker)
    if not ratios_data:
        logger.warning(f"⚠️ Financial data not found for: {ticker}")
        raise HTTPException(status_code=404, detail="Financial data not found")

    # Convert data to FinancialRatios model
    financial_ratios = FinancialRatios(**ratios_data)

    # Log retrieved ratios
    logger.debug(f"📊 Fetched financial ratios for {ticker}: {financial_ratios.dict()}")

    # Analyze stock using Gemini AI
    analysis = analyze_stock(ticker, financial_ratios)
    if "error" in analysis.lower():
        logger.error(f"❌ Gemini analysis failed for {ticker}")

    logger.info(f"✅ Analysis completed for {ticker}")

    return {
        "ticker": ticker,
        "financial_ratios": financial_ratios.dict(),
        "analysis": analysis
    }