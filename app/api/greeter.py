import logging
from fastapi import APIRouter, HTTPException
from app.models.stock import Stock
from app.services.gemini_service import suggest_stocks
from app.utils.logger import logger
import pandas as pd

router = APIRouter(prefix="/greeter")

def get_sp500_tickers():
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df_sp500 = pd.read_html(sp500_url, header=0)[0]
    tickers = df_sp500['Symbol'].str.replace('.', '-', regex=False).tolist()
    return tickers

@router.get("/validate_stock/{stock_query}", response_model=Stock)
async def validate_stock_api(stock_query: str):
    logger.info(f"📡 Received request to validate stock: {stock_query}")
    tickers = get_sp500_tickers()
    stock_query_lower = stock_query.lower()
    for ticker in tickers:
        if ticker.lower() == stock_query_lower:
            logger.info(f"✅ Valid stock found: {ticker}")
            # Replace "Company Name Placeholder" with actual company name if available.
            return Stock(ticker=ticker, company_name="Company Name Placeholder")
    logger.warning(f"❌ Stock not found: {stock_query}")
    raise HTTPException(status_code=404, detail="Stock not found in S&P 500")

@router.get("/stock_suggestions/{user_query}")
async def suggest_stocks_api(user_query: str):
    logger.info(f"📡 Received stock suggestion request: {user_query}")
    suggestions = suggest_stocks(user_query)
    return {"suggestions": suggestions}
