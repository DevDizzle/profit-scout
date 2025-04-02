# -*- coding: utf-8 -*-
import logging
import re
import asyncio
import os
import uuid # Added for generating unique task IDs
import json # Added for SSE data formatting

import httpx
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
# Added for SSE
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Assuming these are correctly placed relative to main.py
from app.services.gemini_service import suggest_stocks
from app.utils.logger import logger

import pandas as pd
from typing import Dict, Tuple, Optional, Any # Added Any
from cachetools import cached, TTLCache

router = APIRouter(prefix="/greeter")

# --- Simple In-Memory Store for Async Results ---
# WARNING: Not suitable for production with multiple workers or restarts.
# Use Redis or a database for robust implementation.
results_store: Dict[str, Any] = {}
# -----------------------------------------------

# Cache for S&P 500 data (TTL set to 1 day, max 1 entry)
sp500_cache = TTLCache(maxsize=1, ttl=86400)

# --- S&P 500 Data Fetching and Caching ---
# (Code unchanged)
@cached(cache=sp500_cache)
def get_sp500_companies() -> Tuple[Dict[str, str], Dict[str, str]]:
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
        ticker_to_company = {}
        company_to_ticker = {}
        for _, row in df_sp500.iterrows():
            ticker_raw = row.get('Symbol'); company_raw = row.get('Security')
            if not ticker_raw or not company_raw: continue
            ticker = str(ticker_raw).replace('.', '-'); company = str(company_raw)
            ticker_lower = ticker.lower(); company_lower = company.lower()
            ticker_to_company[ticker_lower] = company
            company_to_ticker[company_lower] = ticker
        logger.info(f"✅ Fetched {len(ticker_to_company)} S&P 500 companies.")
        return ticker_to_company, company_to_ticker
    except ImportError:
         logger.error("❌ Missing 'lxml'. Install: pip install lxml"); raise HTTPException(500,"Config error: Missing 'lxml'")
    except Exception as e:
        logger.error(f"❌ Failed S&P 500 fetch: {e}", exc_info=True); raise HTTPException(503, f"Could not retrieve S&P 500 data: {e}")

async def get_cached_sp500_data() -> Tuple[Dict[str, str], Dict[str, str]]:
    try: return get_sp500_companies()
    except Exception as e: logger.error(f"❌ Error in dependency: {e}"); raise HTTPException(503, "Failed S&P 500 load.")

# --- Utility Function for Extraction ---
# (Code unchanged)
def extract_stock_info( message: str, ticker_map: Dict[str, str], company_map: Dict[str, str] ) -> Optional[Tuple[str, str]]:
    message_lower = message.lower()
    message_cleaned = re.sub(r'[.,!?;:"\'()\[\]]', '', message_lower)
    words = message_cleaned.split()
    for word in words:
        if word in ticker_map:
            ticker_lower = word; company_name_temp = ticker_map[ticker_lower]
            ticker_original_case = company_map.get(company_name_temp.lower(), word.upper())
            company_name = ticker_map[ticker_lower]
            logger.info(f"✅ Extracted ticker '{ticker_original_case}' for '{company_name}'"); return ticker_original_case, company_name
    sorted_company_names = sorted(company_map.keys(), key=len, reverse=True)
    for company_lower in sorted_company_names:
        try:
            if company_lower and re.search(r'(?<![a-z0-9])' + re.escape(company_lower) + r'(?![a-z0-9])', message_lower):
                ticker = company_map[company_lower]; company_name = ticker_map[ticker.lower()]
                logger.info(f"✅ Extracted company '{company_name}' (Ticker: {ticker})"); return ticker, company_name
        except re.error as e: logger.warning(f"Regex error for '{company_lower}': {e}"); continue
    logger.info("No S&P 500 ticker/company found."); return None

# --- Background Task for Orchestration ---

BASE_API_URL = os.getenv("INTERNAL_API_BASE_URL", "http://localhost:8000")

# Modified background task to accept task_id and store result
async def run_analysis_and_synthesis(ticker: str, task_id: str):
    """
    Background task: Runs quantitative analysis, then calls updated synthesizer.
    Stores the final result (or error) in the results_store using task_id.
    """
    global results_store
    logger.info(f"[Background Task {task_id} - {ticker}] Starting analysis...")
    quantitative_result = None
    final_synthesis_data = None
    status = "error" # Default status

    try:
        async with httpx.AsyncClient(base_url=BASE_API_URL, timeout=300.0) as client:
            # 1. --- Run Quantitative Analysis ---
            quant_url = f"/quantative/analyze_stock/{ticker}" # Note: "quantative" typo assumed intentional
            logger.info(f"[Background Task {task_id} - {ticker}] Calling Quantitative: {quant_url}")
            quant_response = await client.get(quant_url)

            if quant_response.status_code == 200:
                quantitative_result = quant_response.json().get("quantitative_analysis")
                logger.info(f"[Background Task {task_id} - {ticker}] ✅ Quantitative analysis successful.")

                # Ensure result is a dictionary (as expected by synthesizer)
                if not isinstance(quantitative_result, dict):
                     logger.error(f"[Background Task {task_id} - {ticker}] ❌ Quantitative result is not a dict: {type(quantitative_result)}")
                     raise ValueError("Quantitative analysis did not return expected dictionary format.")

                # 2. --- Call Synthesizer ---
                logger.info(f"[Background Task {task_id} - {ticker}] Calling Synthesizer...")
                synthesis_payload = {
                    "ticker": ticker,
                    "yahoo_analysis": quantitative_result # Pass the dict
                }
                synth_response = await client.post("/synthesizer/synthesize", json=synthesis_payload)

                if synth_response.status_code == 200:
                    synthesis_result_data = synth_response.json()
                    final_synthesis_data = {
                        "synthesis": synthesis_result_data.get("synthesis"),
                        "quantitativeData": quantitative_result # Include quant data for UI table
                    }
                    status = "completed"
                    logger.info(f"[Background Task {task_id} - {ticker}] ✅ Synthesis successful.")
                    logger.info(f"[Background Task {task_id} - {ticker}] Final Synthesis: {final_synthesis_data['synthesis'][:100]}...") # Log snippet
                else:
                    logger.error(f"[Background Task {task_id} - {ticker}] ❌ Synthesis failed. Status: {synth_response.status_code}, Response: {synth_response.text}")
                    final_synthesis_data = {"message": f"Synthesis step failed: {synth_response.status_code}"}
            else:
                logger.error(f"[Background Task {task_id} - {ticker}] ❌ Quantitative analysis failed. Status: {quant_response.status_code}, Response: {quant_response.text}")
                final_synthesis_data = {"message": f"Quantitative analysis step failed: {quant_response.status_code}"}

    except Exception as e:
        logger.error(f"[Background Task {task_id} - {ticker}] ❌ Unexpected error during background analysis: {e}", exc_info=True)
        final_synthesis_data = {"message": f"An unexpected error occurred: {e}"}

    # Store the final result (or error message)
    results_store[task_id] = {"status": status, "data": final_synthesis_data}
    logger.info(f"[Background Task {task_id} - {ticker}] Result stored. Task finished.")

# --- SSE Event Generator ---
async def event_generator(task_id: str):
    """
    Yields events for SSE: pings or the final result.
    """
    global results_store
    ping_count = 0
    max_wait_cycles = 180 # Approx 6 minutes (180 cycles * 2 seconds) - adjust as needed
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
                # Send a ping/keep-alive event (optional, helps keep connection open)
                ping_count += 1
                # Limit pings if desired, e.g., every 5 cycles (10 seconds)
                # if ping_count % 5 == 0:
                #    yield "event: ping\ndata: keepalive\n\n"

                cycles_waited += 1
                await asyncio.sleep(2) # Wait before checking again
        else:
             # Loop finished without finding result (timeout)
             logger.warning(f"SSE - Timeout waiting for result for task {task_id}. Closing stream.")
             timeout_result = {"status": "error", "data": {"message": "Analysis timed out."}}
             yield f"data: {json.dumps(timeout_result)}\n\n"
             # Clean up if entry somehow appeared late
             if task_id in results_store:
                 results_store.pop(task_id)

    except asyncio.CancelledError:
         logger.info(f"SSE - Client disconnected for task {task_id}. Cleaning up.")
         # Clean up store if client disconnects before result is ready
         if task_id in results_store:
             results_store.pop(task_id)
         raise # Re-raise cancellation


# --- API Endpoints ---

@router.get("/validate_stock/{stock_query}")
async def validate_stock_api(
    stock_query: str,
    sp500_data: Tuple[Dict[str, str], Dict[str, str]] = Depends(get_cached_sp500_data)
):
    """ Validates if a direct query string is a known S&P 500 ticker or company name. """
    # (Code unchanged)
    logger.info(f"📡 Received direct request to validate stock query: {stock_query}")
    ticker_to_company, company_to_ticker = sp500_data
    query_lower = stock_query.lower().strip()
    if query_lower in ticker_to_company:
        company_name = ticker_to_company[query_lower]
        ticker_original_case = company_to_ticker.get(company_name.lower(), query_lower.upper())
        message = f"Ticker '{ticker_original_case}' recognized for {company_name}. Ready for analysis."
        logger.info(f"✅ Valid ticker found: {ticker_original_case} ({company_name})")
        return {"ticker": ticker_original_case, "company_name": company_name, "message": message, "status": "recognized"}
    elif query_lower in company_to_ticker:
        ticker = company_to_ticker[query_lower]
        company_name = ticker_to_company[ticker.lower()]
        message = f"Company '{company_name}' recognized with ticker {ticker}. Ready for analysis."
        logger.info(f"✅ Valid company found: {company_name} (Ticker: {ticker})")
        return {"ticker": ticker, "company_name": company_name, "message": message, "status": "recognized"}
    else:
        try: suggestions = suggest_stocks(stock_query)
        except Exception as e: logger.error(f"❌ Error getting suggestions: {e}"); suggestions = "(Suggestion service unavailable)"
        message = (f"I couldn't recognize '{stock_query}'. Please use a ticker (e.g., AAPL) or company name (e.g., Apple Inc.).\n\nSuggestions based on your query:\n{suggestions}")
        logger.warning(f"❌ Unrecognized stock query: {stock_query}")
        return {"message": message, "status": "unrecognized", "ticker": None, "company_name": None}


@router.get("/stock_suggestions/{user_query}")
async def stock_suggestions_api(user_query: str):
    """ Returns Gemini-powered stock suggestions based on the user query. """
    # (Code unchanged)
    logger.info(f"📡 Received stock suggestion request: {user_query}")
    try: suggestions = suggest_stocks(user_query); return {"suggestions": suggestions}
    except Exception as e: logger.error(f"❌ Gemini API error: {e}"); raise HTTPException(500,"Failed suggestions.")


class ChatRequest(BaseModel):
    message: str

@router.post("/chat")
async def chat(
    chat_request: ChatRequest,
    background_tasks: BackgroundTasks,
    sp500_data: Tuple[Dict[str, str], Dict[str, str]] = Depends(get_cached_sp500_data)
):
    """
    Handles chat input. Extracts stock info. If found, starts background analysis
    and returns a task_id for SSE streaming. Otherwise, provides guidance.
    """
    user_message = chat_request.message
    logger.info(f"📡 Chat received: {user_message}")
    ticker_to_company, company_to_ticker = sp500_data
    extracted_info = extract_stock_info(user_message, ticker_to_company, company_to_ticker)

    if extracted_info:
        ticker, company_name = extracted_info
        logger.info(f"✅ Stock identified: {company_name} ({ticker}). Starting analysis flow.")

        # Generate unique task ID
        task_id = str(uuid.uuid4())

        # --- Trigger Full Analysis in Background ---
        background_tasks.add_task(run_analysis_and_synthesis, ticker, task_id)
        logger.info(f"Background task {task_id} added for analysis of {ticker}.")
        # ------------------------------------------

        # Return acknowledgment and the task_id for SSE connection
        response_message = f"Okay, I recognized **{company_name} ({ticker})**. I'm starting the analysis. This might take a minute..."
        return {
            "message": response_message,
            "status": "processing_started", # Signal to frontend to connect to SSE
            "ticker": ticker,
            "company_name": company_name,
            "task_id": task_id # ID for the SSE stream
        }
    else:
        # No specific stock recognized, provide guidance.
        logger.info("No valid S&P 500 ticker/company recognized. Providing guidance.")
        try: suggestions = suggest_stocks(user_message)
        except Exception as e: logger.error(f"❌ Error getting suggestions: {e}"); suggestions = "(Suggestion service unavailable)"
        guidance_message = (f"I couldn't identify an S&P 500 stock in your message. Please mention a ticker (like 'AAPL') or company name (like 'Apple Inc.').\n\nSuggestions based on your message:\n{suggestions}")
        return {"message": guidance_message, "status": "needs_clarification", "ticker": None, "company_name": None}

# --- New SSE Endpoint ---
@router.get("/stream/{task_id}")
async def stream_results(task_id: str):
    """
    Endpoint for Server-Sent Events (SSE).
    Clients connect here using the task_id received from /chat.
    Streams keep-alive pings until the result is ready, then sends the result.
    """
    logger.info(f"SSE connection opened for task_id: {task_id}")
    # Note: Consider adding request validation/auth if needed
    return StreamingResponse(event_generator(task_id), media_type="text/event-stream")
