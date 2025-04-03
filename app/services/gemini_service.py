# Refactored Code
import os
import json
import re
from dotenv import load_dotenv
import google.generativeai as genai
from google.generativeai.types import GenerationConfig, GenerateContentResponse # For type hints
from app.utils.logger import logger
from typing import Dict, Optional, Any, Union # Added Union

# Load environment variables from .env file if present
load_dotenv()

# --- Configuration ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    # Use logger for cleaner error reporting instead of raising immediately
    logger.critical("❌ GEMINI_API_KEY environment variable not set. AI features will be disabled.")
    # Depending on application structure, you might raise here or handle downstream
    # raise ValueError("GEMINI_API_KEY not set in environment")
    GEMINI_MODEL_INSTANCE: Optional[genai.GenerativeModel] = None # Ensure it's None if key missing
else:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        # Initialize the Gemini model
        GEMINI_MODEL_INSTANCE = genai.GenerativeModel(
            "gemini-2.0-flash", # Changed model name
            generation_config=GenerationConfig( # Use specific type
                temperature=0.1,
                max_output_tokens=4096 # Synthesis might need many tokens
            )
        )
        logger.info(f"✅ Gemini model '{GEMINI_MODEL_INSTANCE.model_name}' initialized.")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize Gemini model: {e}", exc_info=True)
        GEMINI_MODEL_INSTANCE = None

# --- Helper Functions ---

def _extract_text_from_response(response: Optional[GenerateContentResponse]) -> Optional[str]:
    """Safely extracts text from a Gemini API response."""
    if not response:
        return None
    try:
        # Preferred method via candidates
        if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
             return response.candidates[0].content.parts[0].text.strip()
        # Fallback for simpler text attribute (might exist in some SDK versions/contexts)
        elif hasattr(response, 'text') and response.text:
             return response.text.strip()
    except (AttributeError, IndexError) as e:
        logger.warning(f"⚠️ Could not extract text using standard methods: {e}. Response: {response}")
    return None # Return None if text cannot be extracted

# --- Core Service Functions ---

def suggest_stocks(user_query: str, model: Optional[genai.GenerativeModel] = GEMINI_MODEL_INSTANCE) -> str:
    """
    Generate stock suggestions based on a user query using the Gemini model,
    with a prompt focused on an analytical tone and avoiding disclaimers.
    """
    if not model:
        logger.error("❌ suggest_stocks called but Gemini model is not available.")
        return "⚠️ AI suggestion service is currently unavailable."

    # --- Prompt from previous step ---
    prompt = f"""
Act as a financial analyst specializing in S&P 500 equity research.
Your task is to identify three distinct S&P 500 stocks relevant to the user's query, based on recent, publicly available market data and company performance indicators.

For each suggested stock, provide the following information strictly in this format:
1.  **Ticker Symbol:** [Symbol]
2.  **Company Name:** [Name]
3.  **Rationale:** [A concise (2-3 sentences) analytical explanation focusing on key factors like market position, recent performance highlights, relevant sector trends, or strategic advantages.]

User Query: "{user_query}"

**Instructions & Constraints:**
- Respond in a professional, direct, and analytical tone.
- Focus solely on presenting the requested stock information and rationale.
- Do **not** include conversational filler, introductions, or closings (e.g., avoid starting with "Okay, I understand..." or similar phrases).
- Do **not** include any disclaimers stating you are an AI or cannot provide financial advice. The output is for informational purposes only within a research context.
- Ensure the rationale is brief and data-driven where possible, reflecting recent information (mentioning Q number or year if relevant and available, e.g., "positive Q1 2025 earnings" or "strong performance in 2024").
"""
    # --- End of Prompt ---

    try:
        # **Removed stray URL from here**
        response = model.generate_content(prompt)
        result = _extract_text_from_response(response)

        if not result:
            logger.warning(f"⚠️ Gemini returned an empty response for stock suggestions query: '{user_query}'")
            result = "⚠️ No specific stock suggestions could be generated based on the query."
        else:
            logger.info(f"✅ Gemini stock suggestions generated successfully for query: '{user_query}'")

        return result

    except Exception as e:
        logger.error(f"❌ Gemini API error in suggest_stocks for query '{user_query}': {e}", exc_info=True)
        # **Completed return statement**
        return "⚠️ An error occurred while communicating with the AI to generate stock suggestions."


# Using Union for return type hinting: Dict for success, Dict with error key for failure
AnalyzeResult = Union[Dict[str, Any], Dict[str, str]]

def analyze_yahoo_data(ticker: str, csv_content: str, model: Optional[genai.GenerativeModel] = GEMINI_MODEL_INSTANCE) -> AnalyzeResult:
    """
    Use Gemini to analyze Yahoo Finance CSV data (expected to have section markers).
    Returns a dictionary representing the JSON analysis or an error dictionary.
    """
    if not model:
        logger.error("❌ analyze_yahoo_data called but Gemini model is not available.")
        return {"error": "AI analysis service is currently unavailable."}

    sections: Dict[str, str] = {}
    current_section: Optional[str] = None
    try:
        for line in csv_content.splitlines():
            line = line.strip()
            if line.startswith('[') and line.endswith(']'):
                current_section = line[1:-1].lower() # Use lowercase section names consistently
                sections[current_section] = ""
            elif current_section:
                # Append line to the current section's content
                sections[current_section] += line + "\n"
    except Exception as parse_error:
        logger.error(f"❌ Error parsing CSV content for {ticker}: {parse_error}", exc_info=True)
        return {"error": "Failed to parse input CSV content", "details": str(parse_error)}

    # Basic check if any sections were parsed at all
    if not sections:
         logger.warning(f"⚠️ No sections found in parsed CSV data for {ticker}. CSV content snippet: {csv_content[:200]}")
         return {"error": "No sections found in input data", "ticker": ticker}

    # --- Updated Prompt (incorporating previous change request) ---
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

**Task:**
Analyze the provided financial data sections for the most recent available period. Calculate the specified metrics. Identify the end date of the primary reporting period used for the main financial figures (like total revenue).

**Output Format:**
Return ONLY a valid JSON object containing the following keys:
'revenue_growth', 'latest_revenue', 'previous_revenue',
'debt_to_equity', 'total_debt', 'total_equity',
'fcf_yield', 'operating_cash_flow', 'capital_expenditure', 'free_cash_flow', 'market_cap',
'price_trend_ratio', 'ma_50', 'ma_200',
'reporting_period_ending'.

- The 'reporting_period_ending' value should be a date string, preferably 'YYYY-MM-DD'.
- For metrics that cannot be calculated due to missing data, use the JSON value `null` or the string "N/A".
- Ensure the output is strictly a single JSON object without any surrounding text, explanations, introductions, or markdown formatting like ```json ... ```.
"""
    # --- End of Updated Prompt ---

    try:
        response = model.generate_content(prompt)
        raw_result = _extract_text_from_response(response)

        if not raw_result:
            logger.error(f"❌ Gemini returned empty response for quantitative analysis of {ticker}")
            return {"error": "AI returned empty response", "ticker": ticker}

        # Attempt to strip markdown code fences if present
        # Using a simpler regex approach first
        if raw_result.startswith("```json"):
             result = raw_result.strip()[7:-3].strip() # Strip ```json\n and \n```
        elif raw_result.startswith("```"):
             result = raw_result.strip()[3:-3].strip() # Strip ```\n and \n```
        else:
             result = raw_result # Assume no fences if common patterns don't match

        # Attempt to parse the potentially cleaned JSON
        try:
            result_json = json.loads(result)
            # Basic validation: Check if it's a dictionary
            if not isinstance(result_json, dict):
                 logger.error(f"❌ Gemini response parsed but is not a JSON object (dict) for {ticker}. Type: {type(result_json)}. Parsed data: {result[:500]}")
                 return {"error": "AI response was not a valid JSON object", "raw_response": result, "ticker": ticker}

            logger.info(f"✅ Gemini quantitative analysis parsed as JSON successfully for {ticker}")
            return result_json # Success case
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error in quantitative analysis for {ticker}: {e}. Raw response snippet: {result[:500]}", exc_info=False)
            # Return the raw response in case of decode error for debugging
            return {"error": "Failed to parse AI response as JSON", "raw_response": result, "ticker": ticker}

    except Exception as e:
        logger.error(f"❌ Gemini API error in analyze_yahoo_data for {ticker}: {e}", exc_info=True)
        return {"error": "AI API call failed during analysis", "details": str(e), "ticker": ticker}


def synthesize_analysis(ticker: str, yahoo_analysis: dict, sec_analysis: str, model: Optional[genai.GenerativeModel] = GEMINI_MODEL_INSTANCE) -> str:
    """
    Use Gemini to synthesize qualitative (SEC) and quantitative (Yahoo) analyses.
    """
    if not model:
        logger.error("❌ synthesize_analysis called but Gemini model is not available.")
        return "⚠️ AI synthesis service is currently unavailable."

    # --- Input Validation ---
    if not isinstance(yahoo_analysis, dict):
        logger.warning(f"Synthesize called for {ticker} with invalid yahoo_analysis type: {type(yahoo_analysis)}")
        # Provide a default error structure consistent with successful analysis output if possible
        yahoo_analysis = {"error": "Invalid quantitative data received for synthesis"}

    if not isinstance(sec_analysis, str) or not sec_analysis.strip():
        logger.warning(f"Synthesize called for {ticker} with empty or invalid sec_analysis.")
        sec_analysis = "Qualitative analysis summary was not available or provided."

    # --- Prompt Construction ---
    try:
        # Use indent=None for compactness in the prompt, unless readability is crucial there
        quantitative_metrics_str = json.dumps(yahoo_analysis, indent=None)
    except TypeError as e:
        logger.error(f"❌ Failed to serialize quantitative analysis to JSON for {ticker}: {e}")
        quantitative_metrics_str = json.dumps({"error": "Failed to display quantitative data"})

    # --- Prompt from previous step ---
    prompt = f"""
You are a highly-regarded senior financial analyst, known for rigorous fundamental analysis similar to the principles employed at Berkshire Hathaway. You have been asked by senior investment partners to synthesize the provided Quantitative Metrics and Qualitative Analysis Summary for stock {ticker} into a single, actionable investment recommendation (Buy, Sell, or Hold) based *only* on the provided information.

**Qualitative Financial Analysis Summary:**
{sec_analysis}

**Quantitative Financial Metrics (JSON):**
{quantitative_metrics_str}

**Instructions:**
1.  **Combine Insights**: Integrate the qualitative trends (e.g., revenue growth drivers, profitability improvements) with the quantitative data (e.g., exact revenue figures, ratios) to create a unified analysis. Note any consistencies or contradictions.
2.  **Focus on Financial Performance**: Highlight key financial strengths, trends, and any concerns supported by both sources. Explicitly reference key quantitative metrics (like revenue growth, FCF yield, debt levels) alongside the qualitative context.
3.  **Provide a Recommendation**: Based on the combined analysis, offer a clear Buy, Sell, or Hold recommendation.
4.  **Rationale**: Provide a brief (2-4 sentences) rationale for your recommendation, directly tying it to the synthesized findings from both qualitative and quantitative data.

**Output Format:**
Return a concise summary, preferably using bullet points for the analysis highlights, followed by the Recommendation and Rationale. Avoid extraneous narrative or non-financial details unless they directly impact the investment thesis based on the provided data. Start the response directly with the analysis/summary.
"""
    # --- End of Prompt ---

    try:
        response = model.generate_content(prompt)
        result = _extract_text_from_response(response)

        if not result:
             logger.warning(f"⚠️ Gemini returned empty response for synthesis of {ticker}")
             result = "⚠️ No synthesis could be generated."
        else:
            logger.info(f"✅ Gemini synthesis analysis completed successfully for {ticker}")

        return result

    except Exception as e:
        logger.error(f"❌ Gemini API error in synthesize_analysis for {ticker}: {e}", exc_info=True)
        return "⚠️ An error occurred while synthesizing analyses."
