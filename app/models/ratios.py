from pydantic import BaseModel

class FinancialRatios(BaseModel):
    """Pydantic model for financial ratios used in stock analysis."""
    ROE: float  # Return on Equity
    Debt_to_Equity: float  # Debt-to-Equity Ratio
    Current_Ratio: float  # Current Ratio
    Gross_Margin: float  # Gross Margin
    P_E_Ratio: float  # Price-to-Earnings Ratio
    FCF_Yield: float  # Free Cash Flow Yield