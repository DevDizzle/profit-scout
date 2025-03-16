from fastapi import APIRouter, HTTPException
from app.models.ratios import FinancialRatios
from app.services.bigquery_service import get_financial_ratios
from app.services.gemini_service import analyze_stock
from app.utils.logger import logger

router = APIRouter(prefix="/agent1")  # explicitly set prefix "/agent1"

@router.get("/analyze_stock/{ticker}")
async def analyze_stock_api(ticker: str):
    logger.info(f"📡 Received request to analyze stock: {ticker}")

    # Fetch financial ratios explicitly
    ratios_data = get_financial_ratios(ticker)
    logger.debug(f"📊 Ratios data received from BigQuery: {ratios_data}")

    if not ratios_data:
        logger.warning(f"⚠️ Financial data not found for: {ticker}")
        raise HTTPException(status_code=404, detail="Financial data not found")

    # Explicit Pydantic model validation
    try:
        financial_ratios = FinancialRatios(**ratios_data)
        logger.debug(f"✅ FinancialRatios model successfully created: {financial_ratios.dict()}")
    except Exception as e:
        logger.error(f"❌ Pydantic validation failed for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Pydantic error: {e}")

    # Gemini analysis
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
