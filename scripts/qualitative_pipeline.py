#!/usr/bin/env python3
"""
Qualitative SEC Filing Text Extraction Pipeline for a single stock (AMZN)

Steps:
  1. Load metadata from CSV.
  2. Get most recent filing (10-K/10-Q) for AMZN.
  3. Download the filing HTML.
  4. Extract MD&A and Risk Factors sections.
  5. Clean the text and perform sentiment analysis using Google Cloud Natural Language API.
  6. Upload results to BigQuery table: aialchemy.financial_data.qualitative_sections
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
from datetime import datetime

# Google Cloud imports
from google.cloud import bigquery, language_v1
from google.oauth2 import service_account

# ---------------------------
# CONFIGURATION & LOGGING
# ---------------------------
SLEEP_TIME = (1, 3)
SEC_HEADERS = {
    "User-Agent": "ProfitScout (eraphaelparra@gmail.com)",
    "Accept-Encoding": "gzip, deflate"
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# File paths and table details
METADATA_CSV = "/home/eraphaelparra/profit-scout/data/sp500_metadata.csv"
GCP_KEY_PATH = "/home/eraphaelparra/aialchemy.json"
BQ_TABLE_ID = "aialchemy.financial_data.qualitative_sections"

# ---------------------------
# HELPER FUNCTIONS
# ---------------------------
def load_cik_mapping(metadata_csv: str) -> dict:
    """Load ticker->CIK mapping from CSV with CIK zero-padded to 10 digits."""
    df = pd.read_csv(metadata_csv)
    cik_map = {str(row['ticker']).upper(): str(row['cik']).zfill(10)
               for _, row in df.iterrows()}
    logging.info(f"Loaded CIK mapping for {len(cik_map)} tickers.")
    return cik_map

def get_submission_data_for_ticker(ticker: str, cik_map: dict) -> pd.DataFrame:
    """Fetch SEC submission JSON and return recent filings as DataFrame."""
    cik = cik_map.get(ticker)
    if not cik:
        raise ValueError(f"No CIK found for ticker {ticker}")
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    logging.info(f"Fetching submission data for {ticker} from {url}")
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    data = resp.json().get("filings", {}).get("recent", {})
    return pd.DataFrame(data)

def get_latest_filing_info(ticker: str, cik_map: dict):
    """Return (accession_number, form_type, filing_date) for the most recent 10-K/10-Q filing."""
    try:
        df = get_submission_data_for_ticker(ticker, cik_map)
        relevant = df[df['form'].isin(['10-K', '10-Q'])].copy()
        if relevant.empty:
            logging.warning(f"No 10-K or 10-Q filing found for {ticker}")
            return None, None, None
        relevant['filingDate'] = pd.to_datetime(relevant['filingDate'], errors='coerce')
        recent = relevant.sort_values('filingDate', ascending=False).iloc[0]
        logging.info(f"{ticker}: Latest filing {recent['form']} on {recent['filingDate'].date()}")
        return recent['accessionNumber'], recent['form'], recent['filingDate']
    except Exception as e:
        logging.error(f"Error fetching filing info for {ticker}: {e}")
        return None, None, None

def get_filing_html(cik: str, accession: str) -> str:
    """
    Build the filing index URL from CIK and accession, then download the primary filing document HTML.
    """
    accession_nodash = accession.replace("-", "")
    index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_nodash}/{accession_nodash}-index.htm"
    logging.info(f"Downloading index page: {index_url}")
    try:
        resp = requests.get(index_url, headers=SEC_HEADERS)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
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
        time.sleep(random.uniform(*SLEEP_TIME))
        doc_resp = requests.get(doc_link, headers=SEC_HEADERS)
        doc_resp.raise_for_status()
        return doc_resp.text
    except Exception as e:
        logging.error(f"Error fetching filing HTML: {e}")
        return ""

def extract_text_sections(filing_html: str) -> dict:
    """
    Convert filing HTML to plain text and use regex to extract:
      - MD&A (Item 7)
      - Risk Factors (Item 1A)
    Returns a dictionary with keys "MD&A" and "Risk Factors".
    """
    soup = BeautifulSoup(filing_html, "lxml")
    full_text = soup.get_text(separator=" ", strip=True)
    full_text = re.sub(r'\s+', ' ', full_text)
    
    sections = {}
    # Extract MD&A: from "Item 7" (including "Management's Discussion") until "Item 7A" or "Item 8"
    mda_pattern = re.compile(
        r'item\s*7[\.\:\-\s]+management[’\'`]?s\s+discussion.*?(?=item\s*7a|item\s*8)', 
        re.IGNORECASE | re.DOTALL)
    mda_match = mda_pattern.search(full_text)
    if mda_match:
        sections["MD&A"] = mda_match.group(0).strip()
        logging.info("MD&A section extracted successfully.")
    else:
        logging.warning("MD&A section not found.")
    
    # Extract Risk Factors: from "Item 1A" (Risk Factors) until "Item 1B" or "Item 2"
    risk_pattern = re.compile(
        r'item\s*1a[\.\:\-\s]+risk\s*factors.*?(?=item\s*1b|item\s*2)', 
        re.IGNORECASE | re.DOTALL)
    risk_match = risk_pattern.search(full_text)
    if risk_match:
        sections["Risk Factors"] = risk_match.group(0).strip()
        logging.info("Risk Factors section extracted successfully.")
    else:
        logging.warning("Risk Factors section not found.")
    
    return sections

def analyze_sentiment(text: str, client: language_v1.LanguageServiceClient) -> dict:
    """
    Use Google Cloud Natural Language API to analyze sentiment.
    Returns a dictionary with 'score' and 'magnitude'.
    """
    document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
    try:
        sentiment_response = client.analyze_sentiment(document=document)
        sentiment = sentiment_response.document_sentiment
        logging.info(f"Sentiment analysis: score={sentiment.score}, magnitude={sentiment.magnitude}")
        return {"score": sentiment.score, "magnitude": sentiment.magnitude}
    except Exception as e:
        logging.error(f"Error during sentiment analysis: {e}")
        return {"score": None, "magnitude": None}

def store_to_bigquery(rows: list, client: bigquery.Client):
    """
    Upload rows (list of dicts) to BigQuery.
    """
    if not rows:
        logging.warning("No rows to upload to BigQuery.")
        return
    try:
        errors = client.insert_rows_json(BQ_TABLE_ID, rows)
        if not errors:
            logging.info(f"Successfully uploaded {len(rows)} row(s) to BigQuery.")
        else:
            logging.error(f"BigQuery upload errors: {errors}")
    except Exception as e:
        logging.error(f"BigQuery upload failed: {e}")

# ---------------------------
# MAIN PIPELINE FOR AMZN
# ---------------------------
def main():
    # Process only AMZN
    ticker = "AMZN"
    logging.info(f"Processing qualitative sections for {ticker}...")
    
    cik_map = load_cik_mapping(METADATA_CSV)
    
    # Initialize Google Cloud clients
    credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
    bq_client = bigquery.Client(project=credentials.project_id, credentials=credentials)
    language_client = language_v1.LanguageServiceClient(credentials=credentials)
    
    bq_rows = []
    
    try:
        acc_num, form_type, filing_date = get_latest_filing_info(ticker, cik_map)
        if not acc_num:
            logging.error(f"No valid filing info for {ticker}. Exiting.")
            return
        logging.info(f"{ticker}: Using {form_type} filing dated {filing_date.date()}")
        
        cik = cik_map.get(ticker)
        filing_html = get_filing_html(cik, acc_num)
        if not filing_html:
            logging.error(f"Could not download filing HTML for {ticker}. Exiting.")
            return
        
        sections = extract_text_sections(filing_html)
        if not sections:
            logging.error(f"No sections extracted for {ticker}. Exiting.")
            return
        
        # Process each section (MD&A and Risk Factors)
        for section_name, text_content in sections.items():
            if not text_content:
                logging.warning(f"No text found for section {section_name} of {ticker}.")
                continue
            cleaned_text = re.sub(r'\s+', ' ', text_content).strip()
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
            logging.info(f"{ticker} - {section_name}: Prepared row for BigQuery.")
            
            # For troubleshooting, print first 300 characters of the section
            logging.info(f"{ticker} - {section_name} excerpt: {cleaned_text[:300]}...")
        
        store_to_bigquery(bq_rows, bq_client)
        logging.info("Pipeline for AMZN complete.")
        
    except Exception as e:
        logging.error(f"Error processing {ticker}: {e}")

if __name__ == "__main__":
    main()
