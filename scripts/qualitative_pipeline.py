#!/usr/bin/env python3
"""
Qualitative SEC Filing Text Extraction Pipeline

Steps:
  1. Load metadata (ticker-to-CIK) from CSV.
  2. For each ticker, fetch the most recent 10-K/10-Q filing info via SEC submission JSON.
  3. Construct and download the filing’s HTML document.
  4. Extract MD&A and Risk Factors sections from the plain text (using regex over the text).
  5. Preprocess the text and call Google Cloud Natural Language API to compute sentiment.
  6. Store the results into BigQuery.
  
Requirements:
  - Python 3.x
  - requests, pandas, numpy, bs4, google-cloud-bigquery, google-cloud-language, google-auth, and logging
  - SEC_HEADERS must be set (see below)
  
Make sure your GCP credentials JSON is available (key_path below).
"""

import os
import time
import random
import logging
import requests
import re
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# Google Cloud imports
from google.cloud import bigquery, language_v1
from google.oauth2 import service_account

# ---------------------------
# CONFIGURATION & LOGGING
# ---------------------------
SLEEP_TIME = (1, 3)  # seconds between SEC requests
SEC_HEADERS = {
    "User-Agent": "ProfitScout (eraphaelparra@gmail.com)",
    "Accept-Encoding": "gzip, deflate"
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths (adjust as needed)
METADATA_CSV = "/home/eraphaelparra/profit-scout/data/sp500_metadata.csv"
GCP_KEY_PATH = "/home/eraphaelparra/aialchemy.json"  # your GCP service account JSON
BQ_TABLE_ID = "aialchemy.financial_data.qualitative_sections"  # target BigQuery table

# ---------------------------
# HELPER FUNCTIONS
# ---------------------------
def load_cik_mapping(metadata_csv: str) -> dict:
    """Load ticker->CIK mapping from CSV and zero-pad CIK to 10 digits."""
    df = pd.read_csv(metadata_csv)
    cik_map = {str(row['ticker']).upper(): str(row['cik']).zfill(10)
               for _, row in df.iterrows()}
    logging.info(f"Loaded CIK mapping for {len(cik_map)} tickers.")
    return cik_map

def get_submission_data_for_ticker(ticker: str, cik_map: dict) -> pd.DataFrame:
    """Fetch SEC submission JSON for a given ticker using its CIK and return recent filings as a DataFrame."""
    cik = cik_map.get(ticker)
    if not cik:
        raise ValueError(f"No CIK found for ticker {ticker}")
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    logging.info(f"Fetching submission data for {ticker} from {url}")
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    data = resp.json().get("filings", {}).get("recent", {})
    df = pd.DataFrame(data)
    return df

def get_latest_filing_info(ticker: str, cik_map: dict):
    """
    Return (accession_number, form_type, filing_date) for the most recent filing (10-K or 10-Q).
    """
    try:
        df = get_submission_data_for_ticker(ticker, cik_map)
        relevant = df[df['form'].isin(['10-K', '10-Q'])].copy()
        if relevant.empty:
            logging.warning(f"No 10-K or 10-Q filing found for {ticker}")
            return None, None, None
        # Ensure filingDate is datetime
        relevant['filingDate'] = pd.to_datetime(relevant['filingDate'], errors='coerce')
        recent = relevant.sort_values('filingDate', ascending=False).iloc[0]
        logging.info(f"{ticker}: Latest filing {recent['form']} on {recent['filingDate'].date()}")
        return recent['accessionNumber'], recent['form'], recent['filingDate']
    except Exception as e:
        logging.error(f"Error fetching filing info for {ticker}: {e}")
        return None, None, None

def get_filing_html(cik: str, accession: str) -> str:
    """
    Construct filing URL from CIK and accession number, then download the primary filing document (HTML).
    This function:
      - Removes dashes from accession to form folder name.
      - Downloads the filing index page and then extracts the first document link ending with '.htm'
    """
    accession_nodash = accession.replace("-", "")
    index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_nodash}/{accession_nodash}-index.htm"
    logging.info(f"Downloading index page: {index_url}")
    try:
        resp = requests.get(index_url, headers=SEC_HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # Find the link to the primary document (exclude -index.htm)
        doc_link = None
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.lower().endswith('.htm') and "index" not in href.lower():
                doc_link = "https://www.sec.gov" + href
                break
        if not doc_link:
            logging.error("Primary filing document not found.")
            return ""
        logging.info(f"Primary filing document URL: {doc_link}")
        # Sleep a bit before downloading document
        time.sleep(random.uniform(*SLEEP_TIME))
        doc_resp = requests.get(doc_link, headers=SEC_HEADERS)
        doc_resp.raise_for_status()
        return doc_resp.text
    except Exception as e:
        logging.error(f"Error fetching filing HTML: {e}")
        return ""

def extract_text_sections(filing_html: str) -> dict:
    """
    Given filing HTML content, convert to plain text and use regex to extract:
      - MD&A (Management’s Discussion and Analysis)
      - Risk Factors
    Returns a dict: {'MD&A': text, 'Risk Factors': text}
    """
    # Use BeautifulSoup to strip HTML tags
    soup = BeautifulSoup(filing_html, "lxml")
    full_text = soup.get_text(separator=" ", strip=True)
    # Normalize spaces
    full_text = re.sub(r'\s+', ' ', full_text)
    
    sections = {}

    # Extract MD&A section:
    # Look for "Item 7" followed by "Management's Discussion" and stop at "Item 7A" or "Item 8"
    mda_pattern = re.compile(
        r'item\s*7[\.\:\-\s]+management[’\'`]?s\s+discussion.*?(?=item\s*7a|item\s*8)', re.IGNORECASE | re.DOTALL)
    mda_match = mda_pattern.search(full_text)
    if mda_match:
        mda_text = mda_match.group(0)
        sections["MD&A"] = mda_text.strip()
        logging.info("MD&A section extracted successfully.")
    else:
        logging.warning("MD&A section not found in filing.")

    # Extract Risk Factors section:
    # Look for "Item 1A" followed by "Risk Factors" and stop at "Item 1B" or "Item 2"
    risk_pattern = re.compile(
        r'item\s*1a[\.\:\-\s]+risk\s*factors.*?(?=item\s*1b|item\s*2)', re.IGNORECASE | re.DOTALL)
    risk_match = risk_pattern.search(full_text)
    if risk_match:
        risk_text = risk_match.group(0)
        sections["Risk Factors"] = risk_text.strip()
        logging.info("Risk Factors section extracted successfully.")
    else:
        logging.warning("Risk Factors section not found in filing.")

    return sections

def analyze_sentiment(text: str, client: language_v1.LanguageServiceClient) -> dict:
    """
    Use Google Cloud Natural Language API to analyze sentiment of the given text.
    Returns a dictionary with 'score' and 'magnitude'.
    """
    document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
    try:
        sentiment_response = client.analyze_sentiment(document=document)
        sentiment = sentiment_response.document_sentiment
        logging.info(f"Sentiment analysis score: {sentiment.score}, magnitude: {sentiment.magnitude}")
        return {"score": sentiment.score, "magnitude": sentiment.magnitude}
    except Exception as e:
        logging.error(f"Error during sentiment analysis: {e}")
        return {"score": None, "magnitude": None}

def store_to_bigquery(rows: list, client: bigquery.Client):
    """
    Upload a list of dictionaries to BigQuery.
    Each row should contain: ticker, form_type, filing_date (as ISO date), section, text_content, sentiment_score, sentiment_magnitude.
    """
    if not rows:
        logging.warning("No rows to upload to BigQuery.")
        return
    try:
        errors = client.insert_rows_json(BQ_TABLE_ID, rows)
        if errors == []:
            logging.info(f"Successfully uploaded {len(rows)} rows to BigQuery.")
        else:
            logging.error(f"Errors during BigQuery upload: {errors}")
    except Exception as e:
        logging.error(f"BigQuery upload failed: {e}")

# ---------------------------
# MAIN PIPELINE
# ---------------------------
def main():
    # Load ticker-to-CIK mapping
    cik_map = load_cik_mapping(METADATA_CSV)
    
    # Set up Google Cloud clients
    credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
    bq_client = bigquery.Client(project=credentials.project_id, credentials=credentials)
    language_client = language_v1.LanguageServiceClient(credentials=credentials)
    
    # Prepare list for BigQuery rows
    bq_rows = []
    
    # Load metadata CSV to get tickers list (ensure tickers are uppercase)
    meta_df = pd.read_csv(METADATA_CSV)
    tickers = meta_df['ticker'].str.upper().unique().tolist()
    logging.info(f"Processing qualitative sections for {len(tickers)} tickers...")
    
    # Loop over tickers
    for i, ticker in enumerate(tickers, start=1):
        logging.info(f"[{i}/{len(tickers)}] Processing ticker: {ticker}")
        try:
            # Get the most recent filing info
            acc_num, form_type, filing_date = get_latest_filing_info(ticker, cik_map)
            if not acc_num:
                logging.warning(f"No valid filing info for {ticker}. Skipping.")
                continue
            
            # Only proceed if form is 10-K or 10-Q
            # (we assume MD&A and Risk Factors are present mostly in 10-Ks, but 10-Q may also have MD&A)
            logging.info(f"{ticker}: Using filing {form_type} dated {filing_date.date()}")
            
            # Get filing HTML
            cik = cik_map.get(ticker)
            filing_html = get_filing_html(cik, acc_num)
            if not filing_html:
                logging.error(f"Could not download filing HTML for {ticker}. Skipping.")
                continue
            
            # Extract text sections (MD&A and Risk Factors)
            sections = extract_text_sections(filing_html)
            if not sections:
                logging.warning(f"No sections extracted for {ticker}.")
                continue
            
            # For each section, perform sentiment analysis and prepare row for BigQuery
            for section_name, text_content in sections.items():
                if not text_content:
                    logging.warning(f"No text found for section {section_name} of {ticker}.")
                    continue
                
                # Preprocessing: simple cleaning (remove excessive spaces)
                cleaned_text = re.sub(r'\s+', ' ', text_content).strip()
                
                # Analyze sentiment using Google Cloud NL API
                sentiment = analyze_sentiment(cleaned_text, language_client)
                
                row = {
                    "ticker": ticker,
                    "form_type": form_type,
                    "filing_date": filing_date.strftime("%Y-%m-%d"),
                    "section": section_name,
                    "text_content": cleaned_text,
                    "sentiment_score": sentiment.get("score"),
                    "sentiment_magnitude": sentiment.get("magnitude")
                }
                bq_rows.append(row)
                logging.info(f"{ticker} - {section_name}: Row prepared for BigQuery.")
                
            # Polite random sleep to avoid hitting SEC too fast
            time.sleep(random.uniform(*SLEEP_TIME))
            
        except Exception as e:
            logging.error(f"Error processing ticker {ticker}: {e}")
    
    # Upload all rows to BigQuery
    store_to_bigquery(bq_rows, bq_client)
    logging.info("Qualitative SEC filing pipeline complete.")

if __name__ == "__main__":
    main()
