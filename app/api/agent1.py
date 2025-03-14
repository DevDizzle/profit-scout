from fastapi import APIRouter, HTTPException
from app.models.ratios import FinancialRatios
from app.services.bigquery_service import get_financial_ratios
from app.services.gemini_service import analyze_stock
from app.utils.logger import logger  # Import logger

router = APIRouter(prefix="/agent1")  # Recommended: explicitly set prefix "/agent1"

@router.get("/analyze_stock/{ticker}")
async def analyze_stock_api(ticker: str):
    logger.info(f"📡 Received request to analyze stock: {ticker}")

    # Fetch financial ratios
    ratios_data = get_financial_ratios(ticker)
    if not ratios_data:
        logger.warning(f"⚠️ Financial data not found for: {ticker}")
        raise HTTPException(status_code=404, detail="Financial data not found")

    # Ensure ratios_data correctly maps to FinancialRatios model
    financial_ratios = FinancialRatios(**ratios_data)

    # Analyze using Gemini service
    analysis = analyze_stock(ticker, financial_ratios)
    if "error" in analysis.lower():
        logger.error(f"❌ Gemini analysis failed for {ticker}")
        raise HTTPException(status_code=500, detail="Analysis service error")

    logger.info(f"✅ Analysis completed for {ticker}")

    return {
        "ticker": ticker,
        "financial_ratios": financial_ratios.dict(),
        "analysis": analysis
    }
