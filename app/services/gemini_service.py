import google.generativeai as genai
from app.models.ratios import FinancialRatios
from app.utils.logger import logger  # Import custom logger

# Configure Gemini API
model = genai.GenerativeModel("gemini-1.5-pro")

def suggest_stocks(user_query: str):
    """Generate stock suggestions using Gemini AI."""
    logger.info(f"📡 Sending stock suggestion request to Gemini: {user_query}")

    prompt = f"""
    You are an AI financial assistant. Suggest **three** S&P 500 stocks relevant to:
    
    - User query: "{user_query}"
    
    Ensure **diversification** across sectors. Provide a short **reason** for each pick.
    """

    try:
        response = model.generate_content(
            prompt,
            generation_config={"temperature": 0.5, "max_output_tokens": 200}
        )
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info(f"✅ Gemini stock suggestions received successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error: {e}")
        return "⚠️ An error occurred while generating stock suggestions."

def analyze_stock(ticker: str, ratios: FinancialRatios):
    """Analyze stock using financial ratios and AI."""
    logger.info(f"📡 Sending financial analysis request to Gemini for {ticker}")

    prompt = f"""
    You are a financial AI analyzing {ticker}. Based on the following key financial ratios, determine whether an investor should **BUY, HOLD, or SELL**.

    - **Return on Equity (ROE)**: {ratios.ROE}
    - **Debt to Equity Ratio**: {ratios.Debt_to_Equity}
    - **Current Ratio**: {ratios.Current_Ratio}
    - **Gross Margin**: {ratios.Gross_Margin}
    - **P/E Ratio**: {ratios.P_E_Ratio}
    - **FCF Yield**: {ratios.FCF_Yield}

    Provide a **financial analysis** and a final **BUY, HOLD, or SELL** recommendation.
    """

    try:
        response = model.generate_content(
            prompt,
            generation_config={"temperature": 0.5, "max_output_tokens": 400}
        )
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info(f"✅ Gemini stock analysis received successfully for {ticker}")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error: {e}")
        return "⚠️ An error occurred while generating stock analysis."
