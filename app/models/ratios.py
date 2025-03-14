from typing import Optional
from pydantic import BaseModel

class FinancialRatios(BaseModel):
    """Pydantic model for financial ratios used in stock analysis, flexible for missing data."""
    ROE: Optional[float]  # Return on Equity
    Debt_to_Equity: Optional[float]  # Debt-to-Equity Ratio
    Current_Ratio: Optional[float]  # Current Ratio
    Gross_Margin: Optional[float]  # Gross Margin
    P_E_Ratio: Optional[float]  # Price-to-Earnings Ratio
    FCF_Yield: Optional[float]  # Free Cash Flow Yield
