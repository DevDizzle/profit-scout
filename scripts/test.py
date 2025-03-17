#!/usr/bin/env python3

import os
import logging
import requests
import pandas as pd
from tenacity import retry, wait_exponential, stop_after_attempt

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('raw_data_export')

# SEC API headers
SEC_HEADERS = {
    "User-Agent": "ProfitScout (eraphaelparra@gmail.com)",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov"
}

# Define the CIK mapping for the six target tickers
CIK_MAPPING = {
    "CTVA": "0001755672",
    "AES":  "0000874761",
    "NTAP": "0001002047",
    "GIS":  "0000040704",
    "NOC":  "0001133421",
    "CMI":  "0000026172"
}

def _facts_url(cik: str) -> str:
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"

def _submissions_url(cik: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_facts_json(ticker: str) -> dict:
    """Fetch XBRL company facts JSON from SEC for a given ticker's CIK."""
    cik = CIK_MAPPING.get(ticker.upper())
    if not cik:
        raise ValueError(f"No CIK for ticker {ticker}")
    url = _facts_url(cik)
    logger.debug(f"Fetching facts JSON for {ticker} from {url}")
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    return resp.json()

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_submission_data_for_ticker(ticker: str) -> pd.DataFrame:
    """Fetch recent submission data (including form types, filing dates) for a ticker's CIK."""
    cik = CIK_MAPPING.get(ticker.upper())
    if not cik:
        raise ValueError(f"No CIK for ticker {ticker}")
    url = _submissions_url(cik)
    logger.debug(f"Fetching submission data for {ticker} from {url}")
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    data = resp.json().get("filings", {}).get("recent", {})
    if not data:
        logger.warning(f"No recent filings found in submission data for {ticker}")
        return pd.DataFrame()
    return pd.DataFrame(data)

def get_latest_filing_end_date(ticker: str) -> pd.Timestamp:
    """
    1. Get the recent submission data for 10-K or 10-Q.
    2. Pick the row with the largest filingDate.
    3. Extract the real 'reportPeriod' (the actual fiscal end date).
    """
    sub_df = get_submission_data_for_ticker(ticker)
    if sub_df.empty:
        logger.warning(f"No submission data for {ticker}.")
        return pd.NaT
    
    # Filter to 10-K or 10-Q
    sub_df = sub_df[sub_df['form'].isin(['10-K','10-Q'])].copy()
    if sub_df.empty:
        logger.warning(f"No 10-K or 10-Q in submission data for {ticker}.")
        return pd.NaT

    # Convert string to datetime
    sub_df['filingDate'] = pd.to_datetime(sub_df['filingDate'], errors='coerce')
    # Sort descending by filing date
    sub_df = sub_df.sort_values('filingDate', ascending=False)
    
    # The "reportPeriod" or "periodOfReport" might appear under various columns.
    # Check if we have a "reportPeriod" column:
    # The SEC "recent" object often has "reportDate" or "periodOfReport" as well.
    # We'll do a best-effort approach.
    
    # For demonstration, let's assume "reportPeriod" is in sub_df:
    # (Check the actual JSON structure to confirm.)
    
    possible_cols = ['reportPeriod', 'periodOfReport', 'reportdate']
    # Lower-case columns to handle different capitalizations
    lower_cols = [c.lower() for c in sub_df.columns]
    
    # Find which column might exist
    found_col = None
    for c in possible_cols:
        if c.lower() in lower_cols:
            found_col = c
            break
    
    # If none found, we can't parse the official period end date
    if not found_col:
        logger.warning(f"No 'reportPeriod' or 'periodOfReport' in submission data for {ticker}.")
        return pd.NaT
    
    # Grab the first row (latest filing) and parse its date
    latest_row = sub_df.iloc[0]
    date_str = latest_row.get(found_col, "")
    end_date = pd.to_datetime(date_str, errors='coerce')
    if pd.isna(end_date):
        logger.warning(f"Could not parse a valid date from {found_col} for {ticker}")
    else:
        logger.debug(f"For {ticker}, latest 10-K/10-Q has reporting end date {end_date}")
    return end_date

def facts_to_df(facts_json: dict) -> pd.DataFrame:
    """Convert 'us-gaap' portion of XBRL facts into a DataFrame."""
    rows = []
    gaap_facts = facts_json.get('facts', {}).get('us-gaap', {})
    if not gaap_facts:
        logger.warning("No us-gaap facts found in JSON.")
        return pd.DataFrame()
    for fact_key, fact_val in gaap_facts.items():
        units = fact_val.get('units', {})
        for unit, entries in units.items():
            for entry in entries:
                rows.append({
                    'fact_key': fact_key,
                    'label': fact_val.get('label'),
                    'unit': unit,
                    'value': entry.get('val'),
                    'start': entry.get('start'),
                    'end': entry.get('end'),
                    'fy': entry.get('fy'),
                    'fp': entry.get('fp'),
                    'form': entry.get('form'),
                    'filed': entry.get('filed')
                })
    df = pd.DataFrame(rows)
    logger.debug(f"Converted facts to DataFrame with {len(df)} rows")
    return df

def export_raw_data(tickers, output_file="raw_fact_keys.csv"):
    all_data = []
    for ticker in tickers:
        try:
            logger.info(f"Processing ticker {ticker}...")
            facts_json = get_facts_json(ticker)
            df = facts_to_df(facts_json)
            if df.empty:
                logger.warning(f"No facts for {ticker}, skipping.")
                continue
            
            df['ticker'] = ticker
            # Convert 'end' to a datetime
            df['end'] = pd.to_datetime(df['end'], errors='coerce')
            
            # Figure out the real 'end date' from the latest 10-K or 10-Q
            actual_end_date = get_latest_filing_end_date(ticker)
            if pd.isna(actual_end_date):
                # If we can't parse the official period, fallback to the max
                logger.warning(f"No official period for {ticker}; using fallback max end date.")
                last_date = df['end'].max()
            else:
                # If the official date is found but not in df, fallback
                if actual_end_date in df['end'].values:
                    last_date = actual_end_date
                else:
                    logger.warning(f"{ticker}: official period {actual_end_date} not found in facts. Using fallback.")
                    last_date = df['end'].max()
            
            # Filter to that date
            filtered = df[df['end'] == last_date].copy()
            if filtered.empty:
                logger.warning(f"{ticker}: No rows for end={last_date}, skipping.")
                continue
            
            logger.info(f"For {ticker}, using end={last_date} with {len(filtered)} rows.")
            all_data.append(filtered)
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(output_file, index=False)
        logger.info(f"Exported raw fact keys data for {len(tickers)} tickers to {output_file}")
    else:
        logger.warning("No data to export.")

if __name__ == "__main__":
    target_tickers = ["CTVA", "AES", "NTAP", "GIS", "NOC", "CMI"]
    export_raw_data(target_tickers)
