import logging
from fastapi import APIRouter, HTTPException
from app.models.stock import Stock
from app.services.bigquery_service import validate_stock
from app.services.gemini_service import suggest_stocks
from app.utils.logger import logger  # Import custom logger

router = APIRouter()

@router.get("/validate_stock/{stock_query}", response_model=Stock)
async def validate_stock_api(stock_query: str):
    """Check if a stock exists in S&P 500 and return structured data."""
    logger.info(f"🔍 Validating stock: {stock_query}")
    result = validate_stock(stock_query)

    if result:
        logger.info(f"✅ Stock found: {result['company_name']} ({result['ticker']})")
        return Stock(ticker=result["ticker"], company_name=result["company_name"])
    
    logger.warning(f"❌ Stock not found: {stock_query}")
    raise HTTPException(status_code=404, detail="Stock not found in S&P 500")

@router.get("/stock_suggestions/{user_query}")
async def stock_suggestions_api(user_query: str):
    """Generate stock suggestions using Gemini."""
    logger.info(f"📡 Fetching stock suggestions for query: {user_query}")
    suggestions = suggest_stocks(user_query)
    return {"suggestions": suggestions}
