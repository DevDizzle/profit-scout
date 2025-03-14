# app/api/agent1.py
from fastapi import APIRouter
from app.services.bigquery_service import get_financial_ratios
from app.utils.logger import logger
from app.models.ratios import FinancialRatios
from app.services.gemini_service import analyze_stock

router = APIRouter(prefix="/agent1")

@router.get("/analyze_stock/{ticker}")
async def analyze_stock_api(ticker: str):
    logger.info(f"📡 Received request to analyze stock: {ticker}")

    ratios_data = get_financial_ratios(ticker)
    financial_ratios = FinancialRatios(**ratios_data)
    logger.debug(f"Ratios retrieved: {financial_ratios}")

    analysis = analyze_stock(ticker, financial_ratios)

    return {"financial_ratios": financial_ratios, "analysis": analysis}
