import google.generativeai as genai
from app.models.ratios import FinancialRatios
from app.utils.logger import logger  # Import custom logger

model = genai.GenerativeModel("gemini-2.0-flash")

def suggest_stocks(user_query: str):
    """Generate stock suggestions using Gemini AI."""
    logger.info(f"📡 Sending stock suggestion request to Gemini: {user_query}")

    prompt = f"""
    You are a financial expert. Suggest three diversified S&P 500 stocks based on the following request:
    
    "{user_query}"
    
    Briefly explain why each stock is suitable for the query, ensuring sector diversification.
    """

    try:
        response = model.generate_content(
            prompt,
            generation_config={"temperature": 0.5, "max_output_tokens": 1024}
        )
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info("✅ Gemini stock suggestions received successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error: {e}")
        return "⚠️ An error occurred while generating stock suggestions."

def analyze_stock(ticker: str, ratios: FinancialRatios):
    """Analyze stock using financial ratios and AI."""
    logger.info(f"📡 Sending financial analysis request to Gemini for {ticker}")

    prompt = f"""
    You're a financial analyst providing advice on {ticker} based on these key financial ratios:

    - **Return on Equity (ROE)**: {ratios.ROE}
    - **Debt to Equity Ratio**: {ratios.Debt_to_Equity}
    - **Current Ratio**: {ratios.Current_Ratio}
    - **Gross Margin**: {ratios.Gross_Margin}
    - **P/E Ratio**: {ratios.P_E_Ratio}
    - **FCF Yield**: {ratios.FCF_Yield}

    Provide a concise financial analysis and clearly conclude with a **BUY, HOLD, or SELL** recommendation.
    """

    try:
        response = model.generate_content(
            prompt,
            generation_config={"temperature": 0.5, "max_output_tokens": 1024}
        )
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info(f"✅ Gemini stock analysis received successfully for {ticker}")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error: {e}")
        return "⚠️ An error occurred while generating stock analysis."
