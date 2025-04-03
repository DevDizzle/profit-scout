# -*- coding: utf-8 -*-
import logging
import re
import asyncio
import os
import uuid # Added for generating unique task IDs
import json # Added for SSE data formatting
from typing import Dict, Tuple, Optional, Any, Set # Added Set

import httpx
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
from cachetools import cached, TTLCache

# Placeholder for Gemini Model Type (replace with actual type if available)
# from google.generativeai import GenerativeModel
GenerativeModel = Any # Placeholder type

# Assuming these are correctly placed relative to main.py
from app.services.gemini_service import suggest_stocks # Assumes updated prompt is here
from app.utils.logger import logger

# --- Initialize Gemini Model (Example - Adapt to your application structure) ---
# This should ideally be managed carefully, perhaps using FastAPI's lifespan events
# or a dedicated dependency management system. Avoid recreating it on every request.
# Replace with your actual initialization logic and API key handling.
GEMINI_MODEL_INSTANCE: Optional[GenerativeModel] = None
try:
    # Example initialization - secure API key handling is crucial!
    # import google.generativeai as genai
    # gemini_api_key = os.getenv("GEMINI_API_KEY")
    # if gemini_api_key:
    #     genai.configure(api_key=gemini_api_key)
    #     GEMINI_MODEL_INSTANCE = genai.GenerativeModel('gemini-1.5-flash-latest') # Or your preferred model
    #     logger.info("✅ Gemini model initialized successfully.")
    # else:
    #     logger.warning("⚠️ GEMINI_API_KEY environment variable not set. suggest_stocks will fail.")
    #     # You might want to raise an error or have fallback behavior
    logger.warning("⚠️ Placeholder: Gemini model initialization skipped in this example.")
    # If using a mock/placeholder for development:
    class MockGeminiModel:
        def generate_content(self, prompt): class P: text = f"Mock suggestion for: {prompt[:50]}..."; class C: parts=[P()]; class R: candidates=[C()]; return R() # noqa
    GEMINI_MODEL_INSTANCE = MockGeminiModel() # Use mock for example
    logger.info("✅ Using Mock Gemini model instance.")

except Exception as e:
    logger.error(f"❌ Failed to initialize Gemini model: {e}", exc_info=True)
    GEMINI_MODEL_INSTANCE = None
# --------------------------------------------------------------------------

# Dependency function to provide the initialized model
async def get_gemini_model() -> GenerativeModel:
    if GEMINI_MODEL_INSTANCE is None:
        logger.error("❌ Gemini model dependency requested, but model is not initialized.")
        raise HTTPException(status_code=503, detail="AI suggestion service is unavailable (model not initialized).")
    return GEMINI_MODEL_INSTANCE


router = APIRouter(prefix="/greeter")

# --- Simple In-Memory Store for Async Results ---
results_store: Dict[str, Any] = {}

# --- S&P 500 Data Fetching and Caching ---
sp500_cache = TTLCache(maxsize=1, ttl=86400)

# **MODIFICATION:** Returns uppercase ticker set, lowercase ticker map, lowercase company map
@cached(cache=sp500_cache)
def get_sp500_data() -> Tuple[Set[str], Dict[str, str], Dict[str, str]]:
    """
    Fetches S&P 500 data and returns:
    1. A set of valid UPPERCASE ticker symbols.
    2. A dict mapping lowercase ticker -> Company Name (original case).
    3. A dict mapping lowercase company name -> Ticker Symbol (original case).
    """
    logger.info("Fetching S&P 500 data from Wikipedia (cache miss or expired)...")
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    try:
        df_sp500_list = pd.read_html(sp500_url, header=0, flavor='lxml')
        if not df_sp500_list:
            raise ValueError("No tables found on the Wikipedia page.")
        df_sp500 = df_sp500_list[0]
        if 'Symbol' not in df_sp500.columns or 'Security' not in df_sp500.columns:
            logger.error("❌ S&P 500 Wikipedia table format changed.")
            raise ValueError("Could not find expected columns ('Symbol', 'Security')")

        uppercase_ticker_set = set()
        ticker_lower_to_company = {}
        company_lower_to_ticker = {}

        for _, row in df_sp500.iterrows():
            ticker_raw = row.get('Symbol'); company_raw = row.get('Security')
            if not ticker_raw or not company_raw: continue

            # Clean ticker (replace '.' sometimes found in wiki, e.g., BRK.B) and ensure uppercase
            ticker = str(ticker_raw).replace('.', '-').strip().upper()
            company = str(company_raw).strip()

            if not ticker or not company: continue # Skip if cleaning resulted in empty strings

            uppercase_ticker_set.add(ticker)
            ticker_lower = ticker.lower() # For the lookup map
            company_lower = company.lower()

            ticker_lower_to_company[ticker_lower] = company
            company_lower_to_ticker[company_lower] = ticker # Store original uppercase ticker

        logger.info(f"✅ Fetched {len(uppercase_ticker_set)} S&P 500 companies.")
        return uppercase_ticker_set, ticker_lower_to_company, company_lower_to_ticker
    except ImportError:
        logger.error("❌ Missing 'lxml'. Install: pip install lxml"); raise HTTPException(500,"Config error: Missing 'lxml'")
    except Exception as e:
        logger.error(f"❌ Failed S&P 500 fetch: {e}", exc_info=True); raise HTTPException(503, f"Could not retrieve S&P 500 data: {e}")

# **MODIFICATION:** Updated dependency getter and type hint
async def get_cached_sp500_data() -> Tuple[Set[str], Dict[str, str], Dict[str, str]]:
    """FastAPI dependency to get cached S&P 500 data."""
    try:
        return get_sp500_data()
    except Exception as e:
        logger.error(f"❌ Error getting S&P 500 data dependency: {e}")
        raise HTTPException(status_code=503, detail="Failed to load necessary stock data.")


# --- Utility Function for Extraction ---
# **MODIFICATION:** Rewritten to check for ALL CAPS tickers first
def extract_stock_info(
    message: str,
    uppercase_tickers: Set[str],
    ticker_lower_map: Dict[str, str],
    company_lower_map: Dict[str, str]
) -> Optional[Tuple[str, str]]:
    """
    Extracts stock info (ticker, company name) from a message.
    Prioritizes ALL CAPS tokens matching known S&P 500 tickers.
    Falls back to searching for company names (case-insensitive).

    Returns: Tuple (Ticker, CompanyName) or None
    """
    logger.debug(f"Attempting extraction from: '{message}'")

    # 1. Check for ALL CAPS tickers first
    # Extract potential uppercase words/tokens (simple split, can be improved with regex)
    # Regex avoids splitting on internal hyphens like BRK-B but requires careful crafting
    potential_caps_tickers = re.findall(r'\b[A-Z][A-Z0-9-]{0,5}\b', message) # Find words starting with cap, up to 6 chars with letters/digits/hyphens
    # Filter more strictly
    potential_caps_tickers = [t for t in potential_caps_tickers if t.isupper() and 1 <= len(t) <= 5]

    logger.debug(f"Potential ALL CAPS tickers found: {potential_caps_tickers}")
    for token in potential_caps_tickers:
        if token in uppercase_tickers:
            # Found a direct ALL CAPS ticker match
            company_name = ticker_lower_map.get(token.lower(), "Unknown Company") # Lookup company name
            logger.info(f"✅ Extracted ALL CAPS ticker '{token}' for '{company_name}'")
            return token, company_name # Return the correct case ticker

    logger.debug("No direct ALL CAPS ticker match found. Searching for company name...")
    # 2. Fallback: Search for company names (case-insensitive)
    message_lower = message.lower()
    # Sort by length to match longer names first (e.g., "Alphabet Inc." before "Alphabet")
    sorted_company_names = sorted(company_lower_map.keys(), key=len, reverse=True)

    for company_lower in sorted_company_names:
        # Use word boundaries to avoid partial matches within words
        # Ensure company_lower is not empty and regex compilable
        if not company_lower: continue
        try:
            # Using word boundaries `\b`
            if re.search(r'\b' + re.escape(company_lower) + r'\b', message_lower):
                ticker = company_lower_map[company_lower] # Get the original case ticker
                # Get original case company name using the ticker
                company_name = ticker_lower_map.get(ticker.lower(), company_lower.title()) # Fallback to title case
                logger.info(f"✅ Extracted company '{company_name}' via name search (Ticker: {ticker})")
                return ticker, company_name
        except re.error as e:
            logger.warning(f"Regex error searching for company '{company_lower}': {e}")
            continue # Skip this company name if regex fails

    logger.info("No S&P 500 ticker (ALL CAPS) or company name found in message.")
    return None


# --- Background Task for Orchestration ---
# (Code mostly unchanged, ensure BASE_API_URL is correct)
BASE_API_URL = os.getenv("INTERNAL_API_BASE_URL", "http://localhost:8000")

async def run_analysis_and_synthesis(ticker: str, task_id: str):
    global results_store
    logger.info(f"[Task {task_id} - {ticker}] Starting background analysis...")
    quantitative_result = None
    final_synthesis_data = None
    status = "error"

    try:
        async with httpx.AsyncClient(base_url=BASE_API_URL, timeout=300.0) as client:
            # 1. Quantitative Analysis
            quant_url = f"/quantative/analyze_stock/{ticker}" # Typo "quantative" kept as original
            logger.info(f"[Task {task_id} - {ticker}] GET {quant_url}")
            quant_response = await client.get(quant_url)

            if quant_response.status_code == 200:
                quantitative_result = quant_response.json().get("quantitative_analysis")
                logger.info(f"[Task {task_id} - {ticker}] Quantitative analysis OK.")
                if not isinstance(quantitative_result, dict):
                    logger.error(f"[Task {task_id} - {ticker}] Quant result not a dict: {type(quantitative_result)}")
                    raise ValueError("Quant analysis returned unexpected format.")

                # 2. Synthesizer
                logger.info(f"[Task {task_id} - {ticker}] POST /synthesizer/synthesize")
                synthesis_payload = {"ticker": ticker, "yahoo_analysis": quantitative_result}
                synth_response = await client.post("/synthesizer/synthesize", json=synthesis_payload)

                if synth_response.status_code == 200:
                    synthesis_result_data = synth_response.json()
                    final_synthesis_data = {
                        "synthesis": synthesis_result_data.get("synthesis", "Missing synthesis text."),
                        "quantitativeData": quantitative_result # Pass quant data to UI
                    }
                    status = "completed"
                    logger.info(f"[Task {task_id} - {ticker}] Synthesis OK.")
                    logger.debug(f"[Task {task_id} - {ticker}] Final Data Snippet: {str(final_synthesis_data)[:200]}...")
                else:
                    logger.error(f"[Task {task_id} - {ticker}] Synthesis failed. Status: {synth_response.status_code}, Resp: {synth_response.text[:200]}")
                    final_synthesis_data = {"message": f"Synthesis step failed ({synth_response.status_code})"}
            else:
                logger.error(f"[Task {task_id} - {ticker}] Quantitative analysis failed. Status: {quant_response.status_code}, Resp: {quant_response.text[:200]}")
                final_synthesis_data = {"message": f"Quantitative analysis step failed ({quant_response.status_code})"}

    except httpx.RequestError as e:
         logger.error(f"[Task {task_id} - {ticker}] HTTP request error during background analysis: {e}", exc_info=True)
         final_synthesis_data = {"message": f"Network error connecting to analysis services: {e}"}
    except Exception as e:
        logger.error(f"[Task {task_id} - {ticker}] Unexpected error during background analysis: {e}", exc_info=True)
        final_synthesis_data = {"message": f"An unexpected error occurred: {type(e).__name__}"}

    results_store[task_id] = {"status": status, "data": final_synthesis_data}
    logger.info(f"[Task {task_id} - {ticker}] Stored result with status '{status}'. Task finished.")


# --- SSE Event Generator ---
# (Code unchanged)
async def event_generator(task_id: str):
    global results_store
    ping_count = 0
    max_wait_cycles = 180 # Approx 6 minutes (180 cycles * 2 seconds)
    cycles_waited = 0

    try:
        while cycles_waited < max_wait_cycles:
            if task_id in results_store:
                result_data = results_store.pop(task_id) # Get and remove result
                logger.info(f"SSE - Found result for task {task_id}. Sending.")
                yield f"data: {json.dumps(result_data)}\n\n"
                logger.info(f"SSE - Result sent for task {task_id}. Closing stream.")
                break # Exit loop after sending data

            else:
                # Optional: Send periodic pings if needed by frontend/proxies
                # ping_count += 1
                # yield f"event: ping\ndata: {ping_count}\n\n"
                cycles_waited += 1
                await asyncio.sleep(2) # Wait before checking again
        else:
            # Loop finished without finding result (timeout)
            logger.warning(f"SSE - Timeout waiting for result for task {task_id}. Closing stream.")
            timeout_result = {"status": "error", "data": {"message": "Analysis timed out waiting for results."}}
            yield f"data: {json.dumps(timeout_result)}\n\n"
            if task_id in results_store: # Clean up just in case
                 results_store.pop(task_id)

    except asyncio.CancelledError:
        logger.info(f"SSE - Client disconnected for task {task_id}. Cleaning up.")
        if task_id in results_store: # Clean up store if client disconnects
             results_store.pop(task_id)
        raise # Re-raise cancellation


# --- API Endpoints ---

# **MODIFICATION:** Added Gemini model dependency
@router.get("/validate_stock/{stock_query}")
async def validate_stock_api(
    stock_query: str,
    sp500_data: Tuple[Set[str], Dict[str, str], Dict[str, str]] = Depends(get_cached_sp500_data),
    model: GenerativeModel = Depends(get_gemini_model) # Inject model
):
    """ Validates if a query string is a known S&P 500 ticker (ALL CAPS) or company name. """
    logger.info(f"📡 Received direct validation request: {stock_query}")
    uppercase_tickers, ticker_lower_map, company_lower_map = sp500_data

    # Use the updated extraction logic
    extracted_info = extract_stock_info(stock_query, uppercase_tickers, ticker_lower_map, company_lower_map)

    if extracted_info:
        ticker, company_name = extracted_info
        message = f"'{ticker}' for {company_name} recognized. Ready for analysis."
        logger.info(f"✅ Valid stock identified: {ticker} ({company_name})")
        return {"ticker": ticker, "company_name": company_name, "message": message, "status": "recognized"}
    else:
        # Not recognized, get suggestions
        logger.warning(f"❌ Unrecognized stock query for validation: {stock_query}")
        try:
            # **MODIFICATION:** Pass model instance to suggest_stocks
            suggestions = suggest_stocks(stock_query, model)
        except Exception as e:
            logger.error(f"❌ Error getting suggestions during validation: {e}", exc_info=True)
            suggestions = "(Suggestion service unavailable due to an error.)"

        message = (f"I couldn't directly recognize '{stock_query}' as an S&P 500 ticker (use ALL CAPS like 'AAPL') or company name.\n\nSuggestions based on your query:\n{suggestions}")
        return {"message": message, "status": "unrecognized", "ticker": None, "company_name": None}


# **MODIFICATION:** Added Gemini model dependency
@router.get("/stock_suggestions/{user_query}")
async def stock_suggestions_api(
    user_query: str,
    model: GenerativeModel = Depends(get_gemini_model) # Inject model
):
    """ Returns Gemini-powered stock suggestions based on the user query. """
    logger.info(f"📡 Received stock suggestion request: {user_query}")
    try:
        # **MODIFICATION:** Pass model instance to suggest_stocks
        suggestions = suggest_stocks(user_query, model)
        return {"suggestions": suggestions}
    except Exception as e:
        logger.error(f"❌ Suggestion API error: {e}", exc_info=True)
        # Return 500 if the suggestion itself fails critically
        raise HTTPException(status_code=500, detail=f"Failed to get suggestions: {e}")


class ChatRequest(BaseModel):
    message: str

# **MODIFICATION:** Added Gemini model dependency
@router.post("/chat")
async def chat(
    chat_request: ChatRequest,
    background_tasks: BackgroundTasks,
    sp500_data: Tuple[Set[str], Dict[str, str], Dict[str, str]] = Depends(get_cached_sp500_data),
    model: GenerativeModel = Depends(get_gemini_model) # Inject model
):
    """
    Handles chat input. Extracts stock info (ALL CAPS ticker first).
    Starts background analysis if found, otherwise provides guidance/suggestions.
    """
    user_message = chat_request.message
    logger.info(f"📡 Chat received: {user_message}")
    uppercase_tickers, ticker_lower_map, company_lower_map = sp500_data

    # Use the updated extraction logic
    extracted_info = extract_stock_info(user_message, uppercase_tickers, ticker_lower_map, company_lower_map)

    if extracted_info:
        ticker, company_name = extracted_info
        logger.info(f"✅ Stock identified: {company_name} ({ticker}). Starting analysis flow.")
        task_id = str(uuid.uuid4())
        background_tasks.add_task(run_analysis_and_synthesis, ticker, task_id)
        logger.info(f"Background task {task_id} added for {ticker}.")
        response_message = f"Okay, I recognized **{company_name} ({ticker})**. Starting analysis..."
        return {
            "message": response_message,
            "status": "processing_started",
            "ticker": ticker,
            "company_name": company_name,
            "task_id": task_id
        }
    else:
        # No specific stock recognized, provide guidance/suggestions
        logger.info("No valid S&P 500 ticker/company recognized in chat. Providing guidance.")
        try:
             # **MODIFICATION:** Pass model instance to suggest_stocks
            suggestions = suggest_stocks(user_message, model)
        except Exception as e:
            logger.error(f"❌ Error getting suggestions during chat guidance: {e}", exc_info=True)
            suggestions = "(Suggestion service is temporarily unavailable.)"

        guidance_message = (
            f"I couldn't identify an S&P 500 stock ticker (use ALL CAPS like 'AAPL') or company name in your message."
            f"\n\nSuggestions based on your query:\n{suggestions}"
        )
        return {"message": guidance_message, "status": "needs_clarification", "ticker": None, "company_name": None}

# --- New SSE Endpoint ---
# (Code unchanged)
@router.get("/stream/{task_id}")
async def stream_results(task_id: str):
    """ Endpoint for Server-Sent Events (SSE). """
    logger.info(f"SSE connection opened for task_id: {task_id}")
    return StreamingResponse(event_generator(task_id), media_type="text/event-stream")
