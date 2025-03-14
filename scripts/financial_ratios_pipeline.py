#!/usr/bin/env python3

import os
import time
import random
import logging
import requests
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from tenacity import retry, wait_exponential, stop_after_attempt
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import yfinance as yf

# =============================================================================
#                           CONFIG & LOGGING
# =============================================================================

SLEEP_TIME = (3, 6)       # random sleep for market data
MAX_DAYS_FORWARD = 5      # how many days forward we check from the 'end' date

logger = logging.getLogger('financial_pipeline')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(handler)

SEC_HEADERS = {
    "User-Agent": "ProfitScout (eraphaelparra@gmail.com)",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov"
}

CIK_MAPPING = {}  # global ticker -> padded CIK

# =============================================================================
#                     HELPER FUNCTIONS (SEC + METADATA)
# =============================================================================

def load_cik_mapping(metadata_csv: str) -> dict:
    """
    Read ticker->CIK from CSV, zero-pad the CIK to 10 digits.
    """
    df = pd.read_csv(metadata_csv)
    map_ = {}
    for _, row in df.iterrows():
        tkr = str(row['ticker']).upper()
        cik_str = str(row['cik']).zfill(10)
        map_[tkr] = cik_str
    logger.info(f"Loaded CIK mapping for {len(map_)} tickers.")
    return map_

def _submission_url(cik: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"

def _facts_url(cik: str) -> str:
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_submission_data_for_ticker(ticker: str) -> pd.DataFrame:
    """
    Fetch recent SEC submissions for a single ticker (based on CIK).
    Raises if no data or HTTP error.
    """
    cik = CIK_MAPPING.get(ticker)
    if not cik:
        raise ValueError(f"No CIK for ticker {ticker}")

    url = _submission_url(cik)
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    data = resp.json().get("filings", {}).get("recent", {})
    return pd.DataFrame(data)

def get_latest_filing_info(ticker: str):
    """
    Return (accessionNumber, formType, filingDate) for the most recent 10-K or 10-Q.
    If none, return (None, None, None).
    """
    try:
        df = get_submission_data_for_ticker(ticker)
        relevant = df[df['form'].isin(['10-K', '10-Q'])].copy()
        if relevant.empty:
            logger.warning(f"No 10-K or 10-Q for {ticker}")
            return None, None, None

        relevant['filingDate'] = pd.to_datetime(relevant['filingDate'], errors='coerce')
        recent = relevant.sort_values('filingDate', ascending=False).iloc[0]
        return recent['accessionNumber'], recent['form'], recent['filingDate']
    except Exception as exc:
        logger.error(f"Error fetching filing info for {ticker}: {exc}")
        return None, None, None

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_facts_json(ticker: str) -> dict:
    """
    Fetch XBRL company facts JSON from SEC for a given ticker's CIK.
    """
    cik = CIK_MAPPING.get(ticker)
    if not cik:
        raise ValueError(f"No CIK for ticker {ticker}")

    url = _facts_url(cik)
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    return resp.json()

def facts_to_df(facts_json: dict) -> pd.DataFrame:
    """
    Convert 'us-gaap' portion of XBRL facts into a Pandas DataFrame.
    """
    df = pd.DataFrame()
    gaap_facts = facts_json.get('facts', {}).get('us-gaap', {})
    if not gaap_facts:
        logger.warning("No us-gaap facts found.")
        return df

    for fact_key, fact_val in gaap_facts.items():
        units = fact_val.get('units', {})
        if not units:
            continue
        for _, entries in units.items():
            temp_df = pd.DataFrame(entries)
            # Drop columns and rows that are completely NA
            temp_df = temp_df.dropna(axis=1, how='all')
            temp_df = temp_df.dropna(axis=0, how='all')
            if temp_df.empty:
                continue

            if 'val' in temp_df.columns:
                temp_df['val'] = pd.to_numeric(
                    temp_df['val'].replace('None', np.nan), 
                    errors='coerce'
                )
            temp_df['item'] = fact_val.get('label', fact_key)

            if not temp_df.empty:
                df = pd.concat([df, temp_df], ignore_index=True)

    return df

def get_latest_filing_facts(ticker: str) -> pd.DataFrame:
    """
    1) Get the most recent 10-K/10-Q info (accession, form, filing_date).
    2) Pull the company facts from SEC.
    3) Filter to the last 'end' date in that dataset, ignoring future 'end' > today.
    """
    accession, form_type, filing_date = get_latest_filing_info(ticker)
    if not accession:
        return pd.DataFrame()

    try:
        data_json = get_facts_json(ticker)
        df_facts = facts_to_df(data_json)
        if df_facts.empty:
            return pd.DataFrame()

        df_facts['end'] = pd.to_datetime(df_facts['end'], errors='coerce')
        # Exclude future dates
        today = pd.Timestamp.today().normalize()
        df_facts = df_facts[df_facts['end'] <= today]
        if df_facts.empty:
            logger.info(f"{ticker}: All 'end' dates are in the future, skipping.")
            return pd.DataFrame()

        last_date = df_facts['end'].max()
        recent = df_facts[df_facts['end'] == last_date].copy()

        recent['accn'] = accession
        recent['form'] = form_type
        recent['filed'] = filing_date
        return recent
    except Exception as exc:
        logger.error(f"Error fetching facts for {ticker}: {exc}")
        return pd.DataFrame()

# =============================================================================
#                     PIPELINE: FETCH SEC FINANCIAL METRICS
# =============================================================================

def load_all_financial_metrics(metadata_csv: str) -> pd.DataFrame:
    """
    1) Load metadata
    2) Loop over ALL tickers
    3) Fetch last-filed facts
    4) Return combined DataFrame
    """
    global CIK_MAPPING
    CIK_MAPPING = load_cik_mapping(metadata_csv)

    meta_df = pd.read_csv(metadata_csv)
    tickers = meta_df['ticker'].str.upper().unique().tolist()
    logger.info(f"Fetching SEC metrics for {len(tickers)} tickers...")

    all_frames = []
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"[{i}/{len(tickers)}] Processing ticker: {ticker}")
        try:
            df_facts = get_latest_filing_facts(ticker)
            if df_facts.empty:
                logger.info(f"No valid facts found for {ticker}. Skipping.")
                continue

            logger.info(f"  => Fetched {len(df_facts)} fact rows for {ticker}")

            df_facts['ticker'] = ticker
            # attach metadata columns
            row = meta_df[meta_df['ticker'].str.upper() == ticker].iloc[0]
            df_facts['company_name'] = row.get('company_name', pd.NA)
            df_facts['industry_name'] = row.get('industry_name', pd.NA)
            df_facts['segment'] = row.get('segment', pd.NA)

            all_frames.append(df_facts)
        except Exception as e:
            logger.error(f"Error for ticker {ticker}: {e}")

    if not all_frames:
        logger.info("No data frames were collected from SEC for any ticker.")
        return pd.DataFrame()
    return pd.concat(all_frames, ignore_index=True)

# =============================================================================
#                     PIPELINE: FETCH MARKET DATA (SERIAL)
# =============================================================================

def fetch_single_ticker_market_data(ticker: str, reported_date: pd.Timestamp) -> dict:
    """
    For the given 'reported_date' (the last reported 'end' from SEC),
    we try up to MAX_DAYS_FORWARD to find the first trading day with data.
    Return a *single dict* with:
      [ticker, as_of_date, actual_market_date, market_price, market_cap, dividend].
    """
    if pd.isna(reported_date):
        return {}

    base_date = reported_date.date() if hasattr(reported_date, 'date') else reported_date

    for offset in range(MAX_DAYS_FORWARD + 1):
        try_date = base_date + timedelta(days=offset)
        # skip weekend quickly
        if try_date.weekday() >= 5:
            continue

        # polite, random sleep
        time.sleep(random.uniform(*SLEEP_TIME))

        stock = yf.Ticker(ticker)
        try:
            hist = stock.history(start=try_date, end=try_date + timedelta(days=1), interval='1d')
            if hist.empty:
                continue

            close_price = hist['Close'].iloc[0]
            if pd.isna(close_price):
                continue

            # Attempt to gather market cap from sharesOutstanding
            info = {}
            for _ in range(3):
                try:
                    info = stock.info
                    break
                except Exception as e:
                    logger.warning(f"Retry reading 'stock.info' for {ticker}: {e}")
                    time.sleep(2)

            shares_outstanding = info.get('sharesOutstanding')
            if shares_outstanding:
                market_cap = close_price * float(shares_outstanding)
            else:
                market_cap = info.get('marketCap', np.nan)

            dividend = info.get('dividendRate', 0.0)

            return {
                'ticker': ticker,
                'as_of_date': str(base_date),  # The original 'end' date
                'actual_market_date': str(try_date),
                'market_price': float(close_price),
                'market_cap': float(market_cap) if market_cap else np.nan,
                'dividend': float(dividend),
            }
        except Exception as exc:
            logger.error(f"Error fetching Yahoo data for {ticker} on {try_date}: {exc}")
            continue

    return {}

def fetch_market_data(sec_df: pd.DataFrame) -> pd.DataFrame:
    """
    SERIAL approach: For each unique (ticker, end) pair, fetch exactly one row of market data.
    """
    needed = sec_df[['ticker', 'end']].drop_duplicates()
    needed = needed.dropna(subset=['ticker', 'end'])

    results = []
    total = len(needed)
    logger.info(f"Fetching market data for {total} (ticker, end) pairs (SERIAL).")
    count = 0

    for _, row in needed.iterrows():
        count += 1
        ticker = row['ticker']
        end_date = row['end']
        logger.info(f"[{count}/{total}] Fetching market data for {ticker} (end={end_date.date()})...")
        data_dict = fetch_single_ticker_market_data(ticker, end_date)
        if data_dict:
            logger.info(f"  => Found market data on date={data_dict['actual_market_date']} price={data_dict['market_price']}")
            results.append(data_dict)
        else:
            logger.info(f"  => No market data found for {ticker} near {end_date.date()}")

    if not results:
        logger.warning("No market data results at all!")
        return pd.DataFrame()
    return pd.DataFrame(results)

# =============================================================================
#                     PIPELINE: CALCULATE RATIOS & FINAL OUTPUT
# =============================================================================

def calculate_ratios(sec_df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    """
    1) Pivot SEC data on (ticker, company_name, industry_name, segment, end).
    2) Merge with market_df on (ticker, as_of_date).
    3) Calculate ratios.
    4) Return final with extra columns: industry_name, segment, etc.
    """

    if sec_df.empty:
        logger.warning("SEC DataFrame is empty! No ratios to compute.")
        return pd.DataFrame()

    if market_df.empty:
        logger.warning("Market DataFrame is empty! No ratios to compute.")
        return pd.DataFrame()

    logger.info("Pivoting SEC data to compute ratios...")

    # Fill missing grouping columns so pivot_table won't drop rows
    sec_df['company_name'] = sec_df['company_name'].fillna('UnknownCompany')
    sec_df['industry_name'] = sec_df['industry_name'].fillna('UnknownIndustry')
    sec_df['segment'] = sec_df['segment'].fillna('UnknownSegment')

    pivot_cols = ['ticker', 'company_name', 'industry_name', 'segment', 'end']
    df_sec = sec_df[pivot_cols + ['item', 'val']].copy().dropna(subset=['ticker', 'end', 'item'])

    pivoted = df_sec.pivot_table(
        index=pivot_cols,
        columns='item',
        values='val',
        aggfunc='max'
    ).reset_index()

    pivoted['as_of_date'] = pivoted['end'].dt.date.astype(str)

    # Standard item mapping
    standard_map = {
        "Net Income": [
            "Net Income (Loss) Attributable to Parent",
            "Net Income (Loss) Available to Common Stockholders, Basic"
        ],
        "Total Equity": [
            "Stockholders' Equity Attributable to Parent",
            "StockholdersEquity"
        ],
        "Total Debt": ["Liabilities"],
        "Current Assets": ["Assets, Current"],
        "Current Liabilities": ["Liabilities, Current"],
        "Revenues": [
            "Revenue from Contract with Customer, Excluding Assessed Tax",
            "Revenues"
        ],
        "Cost of Goods Sold": ["Cost of Goods and Services Sold"],
        "Earnings Per Share": ["Earnings Per Share, Basic", "EPSBasic"],
        "Operating Cash Flow": ["Net Cash Provided by (Used in) Operating Activities"],
        "CapEx": ["Payments to Acquire Property, Plant, and Equipment"]
    }

    for std_item, synonyms in standard_map.items():
        found_cols = [c for c in synonyms if c in pivoted.columns]
        if found_cols:
            pivoted[std_item] = pivoted[found_cols].bfill(axis=1).iloc[:, 0]
        else:
            pivoted[std_item] = np.nan

    logger.info(f"Merging pivoted SEC data (shape={pivoted.shape}) with market data (shape={market_df.shape})")
    merged = pd.merge(
        pivoted,
        market_df,
        how='left',
        on=['ticker', 'as_of_date']
    )

    if merged.empty:
        logger.warning("Final merge returned an empty DataFrame. No matching (ticker, as_of_date).")
        return pd.DataFrame()

    # Calculate ratios
    merged['ROE'] = merged['Net Income'] / merged['Total Equity']
    merged['Debt_to_Equity'] = merged['Total Debt'] / merged['Total Equity']
    merged['Current_Ratio'] = merged['Current Assets'] / merged['Current Liabilities']
    merged['Gross_Margin'] = (
        (merged['Revenues'] - merged.get('Cost of Goods Sold', 0.0))
        / merged['Revenues']
    )
    merged['Free_Cash_Flow'] = merged['Operating Cash Flow'] - merged.get('CapEx', 0.0)
    merged['P_E_Ratio'] = merged['market_price'] / merged['Earnings Per Share']
    merged['FCF_Yield'] = merged['Free_Cash_Flow'] / merged['market_cap']

    final_cols = [
        'ticker',
        'company_name',
        'industry_name',
        'segment',
        'as_of_date',
        'market_price',
        'market_cap',
        'dividend',
        'ROE',
        'Debt_to_Equity',
        'Current_Ratio',
        'Gross_Margin',
        'P_E_Ratio',
        'FCF_Yield'
    ]
    final_df = merged[final_cols].drop_duplicates()
    logger.info(f"Final DataFrame shape: {final_df.shape}")
    return final_df

# =============================================================================
#                                 MAIN
# =============================================================================

def main(event=None, context=None):
    """
    1. Load GCP credentials
    2. Read metadata
    3. Fetch SEC data (ALL tickers)
    4. Fetch Yahoo market data (serial)
    5. Calculate ratios
    6. Upload final table to BigQuery
    """
    # 1) Credentials
    key_path = "/home/eraphaelparra/aialchemy.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    client = bigquery.Client(project='aialchemy', credentials=credentials)
    logger.info(f"Using credentials from {key_path}")

    # 2) Metadata CSV
    metadata_csv_path = "/home/eraphaelparra/profit-scout/data/sp500_metadata.csv"

    # 3) Fetch SEC data (for all tickers in CSV)
    sec_df = load_all_financial_metrics(metadata_csv_path)
    if sec_df.empty:
        logger.error("No SEC data returned. Exiting.")
        return
    logger.info(f"SEC DataFrame shape: {sec_df.shape}")

    # 4) Fetch market data
    market_df = fetch_market_data(sec_df)
    if market_df.empty:
        logger.error("No market data fetched. Exiting.")
        return
    logger.info(f"Market DataFrame shape: {market_df.shape}")

    # 5) Calculate ratios
    final_df = calculate_ratios(sec_df, market_df)
    if final_df.empty:
        logger.warning("Final DataFrame is empty; nothing to upload. Exiting.")
        return
    logger.info(f"Final table shape: {final_df.shape}")

    # 6) Upload final table to BigQuery
    table_id = "aialchemy.financial_data.financial_ratios_final_all"
    logger.info(f"Uploading final table to {table_id}...")
    pandas_gbq.to_gbq(
        final_df,
        destination_table=table_id,
        project_id='aialchemy',
        if_exists='replace',
        credentials=credentials
    )
    logger.info(f"Uploaded final table with {len(final_df)} rows to {table_id}. Pipeline complete.")

if __name__ == "__main__":
    main()
