import os
import json
import re
from dotenv import load_dotenv
import google.generativeai as genai
from app.utils.logger import logger

# Load environment variables from .env
load_dotenv()

# Configure Gemini API key from environment variable
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise Exception("GEMINI_API_KEY not set in environment")
genai.configure(api_key=GEMINI_API_KEY)

# Initialize the Gemini model (using a potentially more capable model if needed for synthesis)
# Consider if gemini-1.5-flash is sufficient or if gemini-pro is better
model = genai.GenerativeModel(
    "gemini-2.0-flash",  # Or "gemini-pro"
    generation_config={"temperature": 0.1, "max_output_tokens": 4096}  # Keep tokens high for synthesis
)

def suggest_stocks(user_query: str):
    """
    Return Gemini's stock suggestions based on a user query.
    """
    prompt = f"""
    You are seasoned financial advisor specializing in S&P 500 stocks. Using the latest verified information from Google Search, please suggest three diversified S&P 500 stocks that are relevant to the query below. For each stock, include:

    - The ticker symbol
    - The company name
    - A brief explanation on why it is a good recommendation, considering market trends, financial performance, or industry diversification.

    Query: "{user_query}"

    Ensure your response is concise, factual, and directly references recent, verified data from Google Search.
    """
    try:
        response = model.generate_content(prompt)
        # Add basic response validation/checking if needed
        result = response.text.strip() if hasattr(response, 'text') and response.text else "No response from Gemini"
        logger.info("✅ Gemini stock suggestions received successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error in suggest_stocks: {e}", exc_info=True)
        return "⚠️ An error occurred while generating stock suggestions."

def analyze_yahoo_data(ticker: str, csv_content: str):
    """
    Use Gemini to analyze Yahoo Finance CSV data.
    The CSV content is expected to include section markers like [info], [financials], etc.
    Returns a dictionary representing the JSON analysis or an error dictionary.
    """
    sections = {}
    current_section = None
    try:
        for line in csv_content.splitlines():
            line = line.strip()
            if line.startswith('[') and line.endswith(']'):
                current_section = line[1:-1]
                sections[current_section] = ""
            elif current_section:
                sections[current_section] += line + "\n"
    except Exception as parse_error:
        logger.error(f"❌ Error parsing CSV content for {ticker}: {parse_error}", exc_info=True)
        return {"error": "Failed to parse input CSV content", "details": str(parse_error)}

    # Check if essential sections were parsed (optional but good practice)
    if not sections.get('financials') or not sections.get('balance_sheet'):
        logger.warning(f"⚠️ Missing essential sections ('financials', 'balance_sheet') in parsed CSV data for {ticker}")
        # Depending on strictness, you could return an error here

    prompt = f"""
Here is the financial data divided into sections for stock {ticker}:

[info]
{sections.get('info', 'Not Available')}

[financials]
{sections.get('financials', 'Not Available')}

[balance_sheet]
{sections.get('balance_sheet', 'Not Available')}

[cashflow]
{sections.get('cashflow', 'Not Available')}

[prices]
{sections.get('prices', 'Not Available')}

Parse the data from the most recent period available in the sections. Calculate the following metrics, including the values used in the calculation. If data for a metric is unavailable, state so clearly.

1. **Revenue Growth**: (latest 'Total Revenue' - previous 'Total Revenue') / previous 'Total Revenue' from [financials]. Include 'latest_revenue' and 'previous_revenue'.
2. **Debt-to-Equity Ratio**: latest 'Total Debt' / latest 'Total Stockholder Equity' from [balance_sheet]. Include 'total_debt' and 'total_equity'.
3. **Free Cash Flow Yield**: (latest 'Total Cash From Operating Activities' + latest 'Capital Expenditure') / 'market_cap' from [info]. Calculate 'free_cash_flow' and include 'operating_cash_flow', 'capital_expenditure', 'free_cash_flow', and 'market_cap'. Note: Capital Expenditure might be negative; use the value as provided.
4. **Price Trend Ratio**: 50-day moving average of 'Close' / 200-day moving average of 'Close' from [prices]. Include 'ma_50' and 'ma_200'.

Based *only* on these calculated metrics, provide a preliminary technical recommendation (e.g., Buy signal if Price Trend > 1, Sell signal if < 1, Hold otherwise). Return ONLY a valid JSON object with keys:
'revenue_growth', 'latest_revenue', 'previous_revenue',
'debt_to_equity', 'total_debt', 'total_equity',
'fcf_yield', 'operating_cash_flow', 'capital_expenditure', 'free_cash_flow', 'market_cap',
'price_trend_ratio', 'ma_50', 'ma_200',
'recommendation'.
Use null or "N/A" for values if calculation is not possible due to missing data. Do not include any other text, explanations, or markdown formatting outside the JSON structure.
    """
    try:
        response = model.generate_content(prompt)
        # More robust check for response text
        raw_result = response.text.strip() if hasattr(response, 'text') and response.text else None
        if not raw_result:
            logger.error(f"❌ Gemini returned empty response for quantitative analysis of {ticker}")
            return {"error": "Gemini returned empty response"}

        # Remove markdown code fences if present (e.g., ```json ... ```)
        match = re.match(r"^```(?:json)?\s*(.*?)\s*```$", raw_result, re.DOTALL | re.IGNORECASE)
        result = match.group(1).strip() if match else raw_result

        # Attempt to parse the JSON
        try:
            result_json = json.loads(result)
            logger.info(f"✅ Gemini quantitative analysis parsed as JSON successfully for {ticker}")
            return result_json
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error in quantitative analysis for {ticker}: {e}. Raw response snippet: {result[:500]}", exc_info=False)
            return {"error": "Failed to parse Gemini response as JSON", "raw_response": result}

    except Exception as e:
        logger.error(f"❌ Gemini API error in analyze_yahoo_data for {ticker}: {e}", exc_info=True)
        return {"error": "Gemini API call failed", "details": str(e)}

# --- analyze_pdf_content function removed ---

def synthesize_analysis(ticker: str, yahoo_analysis: dict, sec_analysis: str):
    """
    Use Gemini to synthesize the qualitative (SEC) analysis and quantitative (Yahoo) analysis.
    :param ticker: Stock ticker symbol.
    :param yahoo_analysis: Dictionary with quantitative metrics (output from analyze_yahoo_data).
    :param sec_analysis: String with qualitative analysis summary (fetched from GCS, originally from 10-K).
    """
    # Handle potential errors/missing data in inputs gracefully
    if not isinstance(yahoo_analysis, dict):
        logger.warning(f"Synthesize called for {ticker} with invalid yahoo_analysis type: {type(yahoo_analysis)}")
        yahoo_analysis = {"error": "Invalid quantitative data received"}

    if not isinstance(sec_analysis, str) or not sec_analysis.strip():
        logger.warning(f"Synthesize called for {ticker} with empty or invalid sec_analysis.")
        sec_analysis = "Qualitative analysis was not available or provided."

    # Safely convert quantitative dict to JSON string for the prompt
    try:
        quantitative_metrics_str = json.dumps(yahoo_analysis, indent=2)
    except TypeError as e:
        logger.error(f"❌ Failed to serialize quantitative analysis to JSON for {ticker}: {e}")
        quantitative_metrics_str = json.dumps({"error": "Failed to display quantitative data"})

    # ***** UPDATED PROMPT WITH NEW PERSONA *****
    prompt = f"""
You are a highly-regarded senior financial analyst, known for rigorous fundamental analysis similar to the principles employed at Berkshire Hathaway. You have been asked by senior investment partners to synthesize the provided Quantitative Metrics and Qualitative Analysis Summary for stock {ticker} into a single, actionable investment recommendation (Buy, Sell, or Hold) based *only* on the provided information.

**Qualitative Financial Analysis Summary:**
{sec_analysis}

**Quantitative Financial Metrics (JSON):**
{quantitative_metrics_str}

**Instructions:**
1. **Combine Insights**: Integrate the qualitative trends (e.g., revenue growth drivers, profitability improvements) with the quantitative data (e.g., exact revenue figures, ratios) to create a unified analysis.
2. **Focus on Financial Performance**: Highlight key financial strengths, trends, and any concerns supported by both sources.
3. **Provide a Recommendation**: Based on the combined analysis, offer a clear Buy, Sell, or Hold recommendation with a brief rationale tied to the financial data.

**Output Format:**
Return a concise, bullet-pointed summary of the return analysis and recommendation. Do not include extraneous narrative or non-financial details beyond what impacts financial performance directly.
    """
    try:
        response = model.generate_content(prompt)
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info("✅ Gemini synthesis analysis completed successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error in synthesize_analysis: {e}", exc_info=True)
        return "⚠️ An error occurred while synthesizing analyses."
