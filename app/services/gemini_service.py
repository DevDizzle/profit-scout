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

# Initialize the Gemini model with "gemini-2.0-flash"
model = genai.GenerativeModel(
    "gemini-2.0-flash",
    generation_config={"temperature": 0.1, "max_output_tokens": 8192}
)

def suggest_stocks(user_query: str):
    """
    Return Gemini's stock suggestions based on a user query.
    This version is "grounded" in Google Search results. The prompt instructs the model
    to reference the latest verified information from Google Search.
    """
    prompt = f"""
    You are a financial expert. Using your capabilities and by grounding your answer in Google Search results,
    suggest **three** diversified S&P 500 stocks relevant to the query below. Provide a brief reason for each pick,
    ensuring diversification across sectors.

    Query: "{user_query}"

    Your answer should be grounded in recent and verified data from Google Search.
    """
    try:
        response = model.generate_content(prompt)
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info("✅ Gemini stock suggestions (grounded) received successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error in suggest_stocks: {e}", exc_info=True)
        return "⚠️ An error occurred while generating stock suggestions."

def analyze_yahoo_data(ticker: str, csv_content: str):
    """
    Use Gemini to analyze Yahoo Finance CSV data.
    The CSV content is expected to include section markers like [info], [financials], etc.
    """
    # Parse CSV content into sections
    sections = {}
    current_section = None
    for line in csv_content.splitlines():
        line = line.strip()
        if line.startswith('[') and line.endswith(']'):
            current_section = line[1:-1]
            sections[current_section] = ""
        elif current_section:
            sections[current_section] += line + "\n"

    prompt = f"""
Here is the financial data divided into sections for stock {ticker}:

[info]
{sections.get('info', '')}

[financials]
{sections.get('financials', '')}

[balance_sheet]
{sections.get('balance_sheet', '')}

[cashflow]
{sections.get('cashflow', '')}

[prices]
{sections.get('prices', '')}

Parse the data from the most recent period and calculate the following metrics, including the values used:

1. **Revenue Growth**: (latest 'Total Revenue' - previous 'Total Revenue') / previous 'Total Revenue' from [financials]. Include 'latest_revenue' and 'previous_revenue'.
2. **Debt-to-Equity Ratio**: latest 'Total Debt' / latest 'Total Stockholder Equity' from [balance_sheet]. Include 'total_debt' and 'total_equity'.
3. **Free Cash Flow Yield**: (latest 'Total Cash From Operating Activities' + latest 'Capital Expenditure') / 'market_cap' from [info]. Include 'operating_cash_flow', 'capital_expenditure', 'free_cash_flow', and 'market_cap'.
4. **Price Trend Ratio**: 50-day moving average of 'Close' / 200-day moving average of 'Close' from [prices]. Include 'ma_50' and 'ma_200'.

Based on these metrics, provide a recommendation (Buy, Sell, or Hold). Return only a JSON object with keys: 
'revenue_growth', 'latest_revenue', 'previous_revenue', 
'debt_to_equity', 'total_debt', 'total_equity', 
'fcf_yield', 'operating_cash_flow', 'capital_expenditure', 'free_cash_flow', 'market_cap', 
'price_trend_ratio', 'ma_50', 'ma_200', 
'recommendation'. Do not include any other text or explanations.
    """
    try:
        response = model.generate_content(prompt)
        result = response.text.strip() if response.text else "No response from Gemini"
        
        # Remove markdown code fences if present (e.g., ```json ... ```)
        match = re.match(r"^```(?:json)?\s*(.*?)\s*```$", result, re.DOTALL)
        if match:
            result = match.group(1).strip()
        
        try:
            result_json = json.loads(result)
            logger.info("✅ Gemini quantitative analysis parsed as JSON successfully")
            return result_json
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error in quantitative analysis: {e}. Raw response: {result}", exc_info=True)
            return {"error": "Failed to parse Gemini response as JSON", "raw_response": result}
    except Exception as e:
        logger.error(f"❌ Gemini API error in analyze_yahoo_data: {e}", exc_info=True)
        return {"error": str(e)}

def analyze_pdf_content(ticker: str, pdf_text: str):
    """
    Use Gemini to analyze qualitative SEC filing data (PDF text).
    """
    prompt = f"""
You are an expert financial analyst with deep knowledge of SEC filings. I am providing you with the complete text extracted from an SEC filing for stock {ticker}. Your task is to produce a concise financial analysis summary that focuses exclusively on the company's financial performance and quantitative metrics. Follow these steps:

1. **Extract Key Financial Data**:  
   - Identify and extract the primary financial figures from the income statement (e.g., revenue, net income, margins), balance sheet (e.g., total assets, liabilities, equity), and cash flow statement (e.g., operating cash flow, investing and financing activities).  
   - Note any important ratios or trends directly related to these statements (e.g., growth rates, liquidity or leverage ratios).  

2. **Analyze Financial Trends**:  
   - Summarize significant changes or trends such as revenue growth, profit margin improvements or declines, asset/liability fluctuations, and cash flow trends.  
   - Identify any anomalies or red flags strictly from a financial perspective (e.g., drastic expense increases, unusual debt levels, or declining cash flow).  

3. **Synthesize a Concise Analysis**:  
   - Provide a final summary that is well-structured, focusing solely on the financial performance and outlook of the company.  
   - Avoid details related to non-financial narrative (e.g., general MD&A commentary or risk factors not directly impacting financial metrics) unless they have a direct financial impact.  

4. **Conclude with Financial Recommendations**:  
   - Offer clear, concise recommendations based solely on the financial data, addressing the company’s financial health and any investment considerations.  

Your output should be a concise, bullet-pointed or short-paragraph summary of the financial analysis based on the provided SEC filing text.
    """
    try:
        response = model.generate_content(prompt + "\n\n" + pdf_text)
        result = response.text.strip() if response.text else "No response from Gemini"
        logger.info("✅ Gemini qualitative analysis completed successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Gemini API error in analyze_pdf_content: {e}", exc_info=True)
        return "⚠️ An error occurred while generating SEC filing analysis."

def synthesize_analysis(ticker: str, yahoo_analysis: dict, sec_analysis: str):
    """
    Use Gemini to synthesize the qualitative and quantitative analyses into a single cohesive output.
    :param ticker: Stock ticker symbol.
    :param yahoo_analysis: JSON object (as dict) with quantitative metrics.
    :param sec_analysis: String with qualitative analysis summary.
    """
    quantitative_metrics_str = json.dumps(yahoo_analysis, indent=2)
    prompt = f"""
You are an expert financial analyst tasked with synthesizing a qualitative financial analysis summary and quantitative financial metrics for stock {ticker} into a single, cohesive return analysis and recommendation. Your goal is to combine the narrative insights with the precise metrics to provide a clear and concise summary, focusing solely on financial performance and investment potential.

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
