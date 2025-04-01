import logging
from fastapi import APIRouter
from app.models.stock import Stock
from app.services.gemini_service import suggest_stocks
from app.utils.logger import logger
import pandas as pd

router = APIRouter(prefix="/greeter")

# System prompt defining the greeter's role and purpose.
SYSTEM_PROMPT = (
    "You are FinBot, a knowledgeable financial assistant specializing in S&P 500 stock analysis. "
    "Your purpose is to validate user inputs—whether stock tickers or company names—and guide the user to enter a valid ticker or company. "
    "You answer basic financial questions in a friendly, conversational tone and ensure that downstream analysis only processes valid S&P 500 stocks."
)

def get_sp500_companies():
    """
    Retrieves a mapping of S&P 500 tickers and company names from Wikipedia.
    Returns two dictionaries:
      - ticker_to_company: maps ticker (lowercase) to the full company name.
      - company_to_ticker: maps company name (lowercase) to the ticker.
    """
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df_sp500 = pd.read_html(sp500_url, header=0)[0]
    ticker_to_company = {}
    company_to_ticker = {}
    for _, row in df_sp500.iterrows():
        ticker = row['Symbol'].replace('.', '-')
        company = row['Security']
        ticker_to_company[ticker.lower()] = company
        company_to_ticker[company.lower()] = ticker
    return ticker_to_company, company_to_ticker

@router.get("/system_prompt")
async def system_prompt():
    """
    Returns the system prompt defining the greeter's scope and purpose.
    """
    return {"system_prompt": SYSTEM_PROMPT}

@router.get("/validate_stock/{stock_query}")
async def validate_stock_api(stock_query: str):
    """
    Validates the user input against S&P 500 tickers and company names.
    If the input matches a ticker or company, returns the corresponding ticker and company name.
    Otherwise, returns a friendly message with suggestions and guidance.
    """
    logger.info(f"📡 Received request to validate stock: {stock_query}")
    ticker_to_company, company_to_ticker = get_sp500_companies()
    query_lower = stock_query.lower().strip()

    # Check if the input is a valid ticker
    if query_lower in ticker_to_company:
        company_name = ticker_to_company[query_lower]
        message = f"Ticker '{stock_query.upper()}' recognized for {company_name}. Proceeding to analysis."
        logger.info(f"✅ Valid ticker found: {stock_query.upper()} ({company_name})")
        return {"ticker": stock_query.upper(), "company_name": company_name, "message": message}
    
    # Check if the input is a valid company name
    elif query_lower in company_to_ticker:
        ticker = company_to_ticker[query_lower]
        company_name = ticker_to_company[ticker.lower()]
        message = f"Company '{company_name}' recognized with ticker {ticker}. Proceeding to analysis."
        logger.info(f"✅ Valid company found: {company_name} (Ticker: {ticker})")
        return {"ticker": ticker, "company_name": company_name, "message": message}
    
    else:
        # Use Gemini to provide suggestions based on the query.
        suggestions = suggest_stocks(stock_query)
        message = (
            f"I'm sorry, I couldn't recognize '{stock_query}' as a valid S&P 500 ticker or company name. "
            f"Please enter a valid ticker (e.g., AAPL) or full company name (e.g., Apple Inc.). "
            f"Here are some suggestions based on your input: {suggestions}"
        )
        logger.warning(f"❌ Unrecognized stock query: {stock_query}")
        return {"message": message}

@router.get("/stock_suggestions/{user_query}")
async def stock_suggestions_api(user_query: str):
    """
    Returns Gemini-powered stock suggestions based on the user query.
    """
    logger.info(f"📡 Received stock suggestion request: {user_query}")
    suggestions = suggest_stocks(user_query)
    return {"suggestions": suggestions}

@router.post("/chat")
async def chat(message: str):
    """
    Handles a simple conversational input.
    If the message matches a valid ticker or company, it validates the stock.
    Otherwise, it responds in a friendly tone encouraging the user to provide a ticker or company name.
    """
    logger.info(f"📡 Chat received: {message}")
    ticker_to_company, company_to_ticker = get_sp500_companies()
    message_lower = message.lower().strip()

    if message_lower in ticker_to_company or message_lower in company_to_ticker:
        # Reuse the validate_stock_api logic for recognized tickers/companies.
        return await validate_stock_api(message)
    
    response_message = (
        "I'm FinBot, your financial assistant for S&P 500 stock analysis. "
        "I can help you analyze a stock if you provide a ticker (e.g., AAPL) or a company name (e.g., Apple Inc.). "
        "Could you please provide the correct ticker or company name?"
    )
    logger.info("No valid ticker or company recognized in chat. Prompting user for correct input.")
    return {"message": response_message}
