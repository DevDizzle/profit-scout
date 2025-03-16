import logging
from fastapi import APIRouter, HTTPException
from app.models.stock import Stock
from app.services.bigquery_service import validate_stock
from app.services.gemini_service import suggest_stocks
from app.utils.logger import logger  # Import custom logger

router = APIRouter(prefix="/agent0")

@router.get("/validate_stock/{stock_query}", response_model=Stock)
async def validate_stock_api(stock_query: str):
    logger.info(f"📡 Received request to validate stock: {stock_query}")

    result = validate_stock(stock_query)
    if result:
        logger.info(f"✅ Valid stock found: {result['company_name']} ({result['ticker']})")
        return Stock(ticker=result["ticker"], company_name=result["company_name"])

    logger.warning(f"❌ Stock not found: {stock_query}")
    raise HTTPException(status_code=404, detail="Stock not found in S&P 500")

@router.get("/stock_suggestions/{user_query}")
async def suggest_stocks_api(user_query: str):
    logger.info(f"📡 Received stock suggestion request: {user_query}")

    suggestions = suggest_stocks(user_query)
    return {"suggestions": suggestions}
