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

# Assuming these are correctly placed relative to main.py
from app.services.gemini_service import suggest_stocks # Assuming this handles its own model init
from app.utils.logger import logger

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
    1. A set of valid UPPERCASE ticker symbols (cleaned).
    2. A dict mapping lowercase ticker -> Company Name (original case).
    3. A dict mapping lowercase company name -> Ticker Symbol (original case, cleaned).
    """
    logger.info("Fetching S&P 500 data from Wikipedia (cache miss or expired)...")
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    try:
        # Use lxml for parsing robustness
        df_sp500_list = pd.read_html(sp500_url, header=0, flavor='lxml')
        if not df_sp500_list:
            raise ValueError("No tables found on the Wikipedia page.")
        df_sp500 = df_sp500_list[0]

        # Validate expected columns
        if 'Symbol' not in df_sp500.columns or 'Security' not in df_sp500.columns:
            logger.error("❌ S&P 500 Wikipedia table format changed. Missing 'Symbol' or 'Security'.")
            raise ValueError("Could not find expected columns ('Symbol', 'Security') in Wikipedia table")

        uppercase_ticker_set = set()
        ticker_lower_to_company = {}
        company_lower_to_ticker = {}

        for _, row in df_sp500.iterrows():
            ticker_raw = row.get('Symbol')
            company_raw = row.get('Security')

            # Ensure both ticker and company exist for the row
            if not ticker_raw or not company_raw:
                continue

            # Clean ticker: remove dots (like in BRK.B -> BRK-B), strip whitespace, uppercase
            ticker = str(ticker_raw).replace('.', '-').strip().upper()
            company = str(company_raw).strip()

            # Ensure values are still valid after cleaning
            if not ticker or not company:
                continue

            # Populate the data structures
            uppercase_ticker_set.add(ticker)
            ticker_lower = ticker.lower() # Use cleaned lowercase ticker for map key
            company_lower = company.lower()

            ticker_lower_to_company[ticker_lower] = company # Map lowercase ticker -> Original Company Name
            company_lower_to_ticker[company_lower] = ticker # Map lowercase company -> Original Cleaned UPPERCASE Ticker

        logger.info(f"✅ Fetched {len(uppercase_ticker_set)} S&P 500 companies.")
        return uppercase_ticker_set, ticker_lower_to_company, company_lower_to_ticker
    except ImportError:
        logger.critical("❌ Missing dependency 'lxml'. Please install: pip install lxml")
        raise HTTPException(status_code=500, detail="Server configuration error: Missing 'lxml' library.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch or process S&P 500 data: {e}", exc_info=True)
        # Propagate a server error if essential data cannot be loaded
        raise HTTPException(status_code=503, detail=f"Could not retrieve S&P 500 data: {e}")

# **MODIFICATION:** Updated dependency getter and type hint
async def get_cached_sp500_data() -> Tuple[Set[str], Dict[str, str], Dict[str, str]]:
    """FastAPI dependency to get cached S&P 500 data."""
    try:
        # Call the synchronous cached function
        return get_sp500_data()
    except Exception as e:
        # Log the error from the dependency function itself
        logger.error(f"❌ Error getting S&P 500 data dependency: {e}")
        # Raise HTTPException so FastAPI handles it correctly
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

    Returns: Tuple (Ticker<Original Case>, CompanyName<Original Case>) or None
    """
    logger.debug(f"Attempting stock extraction from message: '{message}'")

    # 1. Check for potential ALL CAPS tickers first
    # Simple split first, then check properties. More robust than complex regex initially.
    # Remove common punctuation that might attach to words first for cleaner splitting.
    message_cleaned_for_tokens = re.sub(r'[.,!?;:"\'()\[\]]', ' ', message)
    words = message_cleaned_for_tokens.split()

    for word in words:
        # Check if the word looks like a ticker (ALL CAPS, appropriate length)
        # AND is in our known uppercase ticker list.
        # This prevents matching lowercase 'a' or general uppercase words.
        if word.isupper() and 1 <= len(word) <= 5 and word in uppercase_tickers:
            # Found an ALL CAPS ticker match
            ticker_found = word # Already correct case
            # Look up the company name using the lowercase version of the found ticker
            company_name = ticker_lower_map.get(ticker_found.lower(), "Unknown Company")
            logger.info(f"✅ Extracted ALL CAPS ticker '{ticker_found}' for '{company_name}'")
            return ticker_found, company_name

    logger.debug("No direct ALL CAPS ticker match found. Searching for company name...")
    # 2. Fallback: Search for company names (case-insensitive)
    message_lower = message.lower()
    # Sort by length to match longer names first (e.g., "Alphabet Inc." before "Alphabet")
    sorted_company_names = sorted(company_lower_map.keys(), key=len, reverse=True)

    for company_lower in sorted_company_names:
        # Ensure company_lower is not empty
        if not company_lower: continue
        try:
            # Use word boundaries `\b` to avoid partial matches within words
            # Ensure the company name itself doesn't break the regex
            if re.search(r'\b' + re.escape(company_lower) + r'\b', message_lower):
                # Found company name match, get the original case ticker from the map
                ticker = company_lower_map[company_lower] # Ticker is stored in correct (upper)case
                # Get original case company name using the *lowercase* version of the ticker
                company_name = ticker_lower_map.get(ticker.lower(), company_lower.title()) # Fallback to title case
                logger.info(f"✅ Extracted company '{company_name}' via name search (Ticker: {ticker})")
                return ticker, company_name
        except re.error as e:
            logger.warning(f"Regex error searching for company '{company_lower}': {e}")
            continue # Skip this company name if regex fails

    logger.info(f"No S&P 500 ticker (ALL CAPS) or company name found in message: '{message}'")
    return None


# --- Background Task for Orchestration ---
# (Code unchanged from original)
BASE_API_URL = os.getenv("INTERNAL_API_BASE_URL", "http://localhost:8000")

async def run_analysis_and_synthesis(ticker: str, task_id: str):
    global results_store
    logger.info(f"[Background Task {task_id} - {ticker}] Starting analysis...")
    quantitative_result = None
    final_synthesis_data = None
    status = "error" # Default status

    try:
        async with httpx.AsyncClient(base_url=BASE_API_URL, timeout=300.0) as client:
            # 1. --- Run Quantitative Analysis ---
            quant_url = f"/quantative/analyze_stock/{ticker}" # Typo kept as original
            logger.info(f"[Background Task {task_id} - {ticker}] Calling Quantitative: {quant_url}")
            quant_response = await client.get(quant_url)

            if quant_response.status_code == 200:
                quantitative_result = quant_response.json().get("quantitative_analysis")
                logger.info(f"[Background Task {task_id} - {ticker}] ✅ Quantitative analysis successful.")
                if not isinstance(quantitative_result, dict):
                    logger.error(f"[Background Task {task_id} - {ticker}] ❌ Quantitative result is not a dict: {type(quantitative_result)}")
                    raise ValueError("Quantitative analysis did not return expected dictionary format.")

                # 2. --- Call Synthesizer ---
                logger.info(f"[Background Task {task_id} - {ticker}] Calling Synthesizer...")
                synthesis_payload = {
                    "ticker": ticker,
                    "yahoo_analysis": quantitative_result
                }
                synth_response = await client.post("/synthesizer/synthesize", json=synthesis_payload)

                if synth_response.status_code == 200:
                    synthesis_result_data = synth_response.json()
                    final_synthesis_data = {
                        "synthesis": synthesis_result_data.get("synthesis"),
                        "quantitativeData": quantitative_result
                    }
                    status = "completed"
                    logger.info(f"[Background Task {task_id} - {ticker}] ✅ Synthesis successful.")
                    logger.debug(f"[Background Task {task_id} - {ticker}] Final Synthesis Snippet: {str(final_synthesis_data)[:150]}...")
                else:
                    logger.error(f"[Background Task {task_id} - {ticker}] ❌ Synthesis failed. Status: {synth_response.status_code}, Response: {synth_response.text[:200]}")
                    final_synthesis_data = {"message": f"Synthesis step failed: {synth_response.status_code}"}
            else:
                logger.error(f"[Background Task {task_id} - {ticker}] ❌ Quantitative analysis failed. Status: {quant_response.status_code}, Response: {quant_response.text[:200]}")
                final_synthesis_data = {"message": f"Quantitative analysis step failed: {quant_response.status_code}"}

    except httpx.RequestError as exc:
         logger.error(f"[Background Task {task_id} - {ticker}] ❌ HTTP Request error during analysis: {exc}")
         final_synthesis_data = {"message": f"Network error during analysis: {exc}"}
    except Exception as e:
        logger.error(f"[Background Task {task_id} - {ticker}] ❌ Unexpected error during background analysis: {e}", exc_info=True)
        final_synthesis_data = {"message": f"An unexpected server error occurred: {type(e).__name__}"}

    # Store the final result (or error message)
    results_store[task_id] = {"status": status, "data": final_synthesis_data}
    logger.info(f"[Background Task {task_id} - {ticker}] Result stored with status '{status}'. Task finished.")

# --- SSE Event Generator ---
# (Code unchanged from original)
async def event_generator(task_id: str):
    global results_store
    ping_count = 0
    max_wait_cycles = 180 # Approx 6 minutes
    cycles_waited = 0

    try:
        while cycles_waited < max_wait_cycles:
            if task_id in results_store:
                result_data = results_store.pop(task_id)
                logger.info(f"SSE - Found result for task {task_id}. Sending.")
                yield f"data: {json.dumps(result_data)}\n\n"
                logger.info(f"SSE - Result sent for task {task_id}. Closing stream.")
                break

            else:
                # Optional keep-alive ping
                # logger.debug(f"SSE - Waiting for task {task_id} ({cycles_waited}/{max_wait_cycles})")
                cycles_waited += 1
                await asyncio.sleep(2)
        else:
            # Loop finished without finding result (timeout)
            logger.warning(f"SSE - Timeout waiting for result for task {task_id}. Closing stream.")
            timeout_result = {"status": "error", "data": {"message": "Analysis timed out."}}
            yield f"data: {json.dumps(timeout_result)}\n\n"
            if task_id in results_store: # Clean up just in case
                 results_store.pop(task_id)

    except asyncio.CancelledError:
        logger.info(f"SSE - Client disconnected for task {task_id}. Cleaning up.")
        if task_id in results_store:
             results_store.pop(task_id)
        raise # Re-raise cancellation


# --- API Endpoints ---

# **MODIFICATION:** Updated dependency type hint and unpacking
@router.get("/validate_stock/{stock_query}")
async def validate_stock_api(
    stock_query: str,
    sp500_data: Tuple[Set[str], Dict[str, str], Dict[str, str]] = Depends(get_cached_sp500_data)
):
    """ Validates if a query is a known S&P 500 ticker (ALL CAPS) or company name. """
    logger.info(f"📡 Received direct request to validate stock query: {stock_query}")
    # Unpack the data from the dependency
    uppercase_tickers, ticker_lower_map, company_lower_map = sp500_data

    # Use the updated extraction function
    extracted_info = extract_stock_info(stock_query, uppercase_tickers, ticker_lower_map, company_lower_map)

    if extracted_info:
        ticker, company_name = extracted_info
        message = f"Ticker '{ticker}' recognized for {company_name}. Ready for analysis."
        logger.info(f"✅ Valid stock found via validation endpoint: {ticker} ({company_name})")
        return {"ticker": ticker, "company_name": company_name, "message": message, "status": "recognized"}
    else:
        # Not recognized directly, provide suggestions
        logger.warning(f"❌ Unrecognized stock query via validation endpoint: {stock_query}")
        try:
            # NOTE: Assumes suggest_stocks handles its own model instance internally
            suggestions = suggest_stocks(stock_query)
        except Exception as e:
            logger.error(f"❌ Error getting suggestions during validation: {e}", exc_info=True)
            suggestions = "(Suggestion service unavailable)"

        message = (
            f"I couldn't recognize '{stock_query}' as an S&P 500 ticker (use ALL CAPS like 'AAPL') or company name."
            f"\n\nSuggestions based on your query:\n{suggestions}"
        )
        return {"message": message, "status": "unrecognized", "ticker": None, "company_name": None}


# (Endpoint unchanged from original - assumes suggest_stocks handles model)
@router.get("/stock_suggestions/{user_query}")
async def stock_suggestions_api(user_query: str):
    """ Returns Gemini-powered stock suggestions based on the user query. """
    logger.info(f"📡 Received stock suggestion request: {user_query}")
    try:
        # NOTE: Assumes suggest_stocks handles its own model instance internally
        suggestions = suggest_stocks(user_query)
        return {"suggestions": suggestions}
    except Exception as e:
        logger.error(f"❌ Gemini API error in suggestions endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get suggestions.")


class ChatRequest(BaseModel):
    message: str

# **MODIFICATION:** Updated dependency type hint and unpacking, uses updated extract_stock_info
@router.post("/chat")
async def chat(
    chat_request: ChatRequest,
    background_tasks: BackgroundTasks,
    sp500_data: Tuple[Set[str], Dict[str, str], Dict[str, str]] = Depends(get_cached_sp500_data)
):
    """
    Handles chat input. Extracts stock info (ALL CAPS ticker first).
    Starts background analysis if found, otherwise provides guidance/suggestions.
    """
    user_message = chat_request.message
    logger.info(f"📡 Chat received: {user_message}")
    # Unpack the data from the dependency
    uppercase_tickers, ticker_lower_map, company_lower_map = sp500_data

    # Use the updated extraction logic
    extracted_info = extract_stock_info(user_message, uppercase_tickers, ticker_lower_map, company_lower_map)

    if extracted_info:
        # Stock identified (either ALL CAPS ticker or company name)
        ticker, company_name = extracted_info
        logger.info(f"✅ Stock identified in chat: {company_name} ({ticker}). Starting analysis flow.")
        task_id = str(uuid.uuid4())
        background_tasks.add_task(run_analysis_and_synthesis, ticker, task_id)
        logger.info(f"Background task {task_id} added for analysis of {ticker}.")
        response_message = f"Okay, I recognized **{company_name} ({ticker})**. Starting analysis..."
        return {
            "message": response_message,
            "status": "processing_started", # Signal to frontend to connect to SSE
            "ticker": ticker,
            "company_name": company_name,
            "task_id": task_id # ID for the SSE stream
        }
    else:
        # No specific stock recognized, provide guidance/suggestions
        logger.info("No valid S&P 500 ticker (ALL CAPS) or company name recognized in chat. Providing guidance.")
        try:
            # NOTE: Assumes suggest_stocks handles its own model instance internally
            suggestions = suggest_stocks(user_message)
        except Exception as e:
            logger.error(f"❌ Error getting suggestions during chat: {e}", exc_info=True)
            suggestions = "(Suggestion service unavailable)"

        guidance_message = (
            f"I couldn't identify an S&P 500 stock ticker (use ALL CAPS like 'AAPL') or company name in your message."
            f"\n\nSuggestions based on your query:\n{suggestions}"
        )
        return {"message": guidance_message, "status": "needs_clarification", "ticker": None, "company_name": None}

# --- SSE Endpoint ---
# (Code unchanged from original)
@router.get("/stream/{task_id}")
async def stream_results(task_id: str):
    """ Endpoint for Server-Sent Events (SSE). """
    logger.info(f"SSE connection opened for task_id: {task_id}")
    return StreamingResponse(event_generator(task_id), media_type="text/event-stream")
