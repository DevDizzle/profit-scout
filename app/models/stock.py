from pydantic import BaseModel

class Stock(BaseModel):
    """Pydantic model for validated stock information."""
    ticker: str  # Stock symbol (e.g., AAPL, TSLA)
    company_name: str  # Full company name (e.g., Apple Inc.)