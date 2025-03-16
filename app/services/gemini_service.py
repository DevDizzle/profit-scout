import os
from dotenv import load_dotenv
import google.generativeai as genai
from app.utils.logger import logger
from app.models.ratios import FinancialRatios

load_dotenv()  # explicitly loads from .env file

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

model = genai.GenerativeModel(
    "gemini-2.0-flash",
    generation_config={"temperature": 0.5, "max_output_tokens": 1024}
)

def build_analysis_prompt(ticker: str, ratios: FinancialRatios):
    prompt = f"You are an expert financial analyst evaluating {ticker}.\n\n"

    ratios_dict = ratios.dict(exclude_none=True)
    missing_ratios = [field for field, value in ratios.dict().items() if value is None]

    # Include available ratios explicitly
    if ratios_dict:
        prompt += "Available financial ratios:\n"
        for name, value in ratios_dict.items():
            formatted_name = name.replace("_", " ")
            prompt += f"- **{formatted_name}**: {value}\n"
    else:
        prompt += "Note: Limited financial ratios available for analysis.\n"

    # Mention missing data explicitly
    if missing_ratios:
        prompt += "\nThe following ratios are missing:\n"
        for name in missing_ratios:
            prompt += f"- {name.replace('_', ' ')}\n"

    prompt += (
        "\nUsing the available information, provide a concise financial "
        "analysis and conclude with a clear BUY, HOLD, or SELL recommendation. "
        "If the data is insufficient for a definitive recommendation, clearly state that."
    )

    return prompt


def suggest_stocks(user_query: str):
    """Generate stock suggestions using Gemini AI."""
    logger.info(f"📡 Sending stock suggestion request to Gemini: {user_query}")

    prompt = f"""
    You are a financial expert. Suggest **three** diversified S&P 500 stocks relevant to:

    "{user_query}"

    Provide a brief reason for each pick, ensuring diversification across sectors.
    """

    try:
        response = model.generate_content(prompt)
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info("✅ Gemini stock suggestions received successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error: {e}")
        return "⚠️ An error occurred while generating stock suggestions."


def analyze_stock(ticker: str, ratios: FinancialRatios):
    """Analyze stock using financial ratios."""
    logger.info(f"📡 Sending financial analysis request to Gemini for {ticker}")

    prompt = build_analysis_prompt(ticker, ratios)

    try:
        response = model.generate_content(prompt)
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info(f"✅ Gemini stock analysis received successfully for {ticker}")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error: {e}")
        return "⚠️ An error occurred while generating stock analysis."
