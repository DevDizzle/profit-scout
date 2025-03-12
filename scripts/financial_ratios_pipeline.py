#!/usr/bin/env python3

import os
import time
import random
import logging
import requests
import pandas as pd
import pandas_gbq
import yfinance as yf
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, wait_exponential, stop_after_attempt

# === CONFIGURABLE PARAMETERS ===
SLEEP_TIME = (1, 3)  # Random sleep between 1 and 3 seconds after each ticker
MAX_WORKERS = 10  # Number of parallel workers for fetching data
CHUNK_SIZE = 50  # Number of tickers per batch for uploading to BigQuery
# ===============================

# Set up logging
logger = logging.getLogger('financial_pipeline')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# SEC API headers
headers = {
    "User-Agent": "ProfitScout (eraphaelparra@gmail.com)",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov"
}
CIK_MAPPING = {}

def load_cik_mapping(metadata_csv):
    """Load ticker -> CIK mapping from CSV, padding CIK with leading zeros."""
    df = pd.read_csv(metadata_csv)
    cik_map = {row['ticker'].upper(): str(row['cik']).zfill(10) for _, row in df.iterrows()}
    logger.info(f"Loaded CIK mapping for {len(cik_map)} tickers.")
    return cik_map

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_submission_data_for_ticker(ticker):
    """Fetch recent filings from SEC submissions endpoint with retry logic."""
    cik = CIK_MAPPING.get(ticker)
    if not cik:
        logger.error(f"No CIK found for ticker {ticker}")
        raise ValueError(f"No CIK for {ticker}")
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return pd.DataFrame(response.json().get("filings", {}).get("recent", {}))

def get_latest_filing(ticker):
    """Get the latest 10-Q or 10-K filing by filing date."""
    try:
        df = get_submission_data_for_ticker(ticker)
        filtered_df = df[df['form'].isin(['10-Q', '10-K'])].copy()
        if filtered_df.empty:
            logger.warning(f"No 10-Q or 10-K filings found for {ticker}")
            return None, None, None
        filtered_df['filed'] = pd.to_datetime(filtered_df['filingDate'], errors='coerce')
        recent_filing = filtered_df.sort_values('filed', ascending=False).iloc[0]
        logger.info(f"Latest filing for {ticker}: {recent_filing['form']} on {recent_filing['filed'].date()}")
        return recent_filing['accessionNumber'], recent_filing['form'], recent_filing['filed']
    except Exception as e:
        logger.error(f"Error fetching latest filing for {ticker}: {e}")
        return None, None, None

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_facts(ticker):
    """Fetch XBRL company facts from SEC with retry logic."""
    cik = CIK_MAPPING.get(ticker)
    if not cik:
        logger.error(f"No CIK found for ticker {ticker}")
        raise ValueError(f"No CIK for {ticker}")
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def facts_to_df(company_facts):
    """Convert XBRL facts into a DataFrame, handling 'None' as NaN."""
    df = pd.DataFrame()
    facts = company_facts.get('facts', {}).get('us-gaap', {})
    if not facts:
        logger.warning("No us-gaap facts found.")
        return df
    for report in facts.values():
        units = report.get('units', {})
        if not units:
            continue
        for unit_data in units.values():
            temp_df = pd.DataFrame(unit_data)
            temp_df = temp_df.dropna(axis=1, how='all')
            if temp_df.empty:
                continue
            if 'val' in temp_df.columns:
                temp_df['val'] = pd.to_numeric(temp_df['val'].replace('None', np.nan), errors='coerce')
            temp_df['item'] = report.get('label')
            df = pd.concat([df, temp_df], ignore_index=True)
    return df

def get_latest_metrics(ticker):
    """Fetch latest financial metrics from the most recent filing."""
    logger.info(f"Fetching metrics for {ticker}...")
    accession_number, form_type, filing_date = get_latest_filing(ticker)
    if not accession_number:
        return pd.DataFrame()
    try:
        facts = get_facts(ticker)
        df_facts = facts_to_df(facts)
        if df_facts.empty:
            logger.warning(f"No facts retrieved for {ticker} from {form_type} (filed: {filing_date}).")
            return pd.DataFrame()
        df_facts['end'] = pd.to_datetime(df_facts['end'], errors='coerce')
        recent_date = df_facts['end'].max()
        recent_facts = df_facts[df_facts['end'] == recent_date].copy()
        recent_facts['accn'] = accession_number
        recent_facts['form'] = form_type
        recent_facts['filed'] = pd.to_datetime(filing_date, errors='coerce')
        logger.info(f"Retrieved {len(recent_facts)} records for {ticker} from {form_type} (filed: {filing_date}).")
        return recent_facts
    except Exception as e:
        logger.error(f"Error processing facts for {ticker}: {e}")
        return pd.DataFrame()

def load_all_financial_metrics(metadata_csv):
    """Fetch SEC data for all tickers in parallel."""
    global CIK_MAPPING
    metadata_df = pd.read_csv(metadata_csv)
    CIK_MAPPING = load_cik_mapping(metadata_csv)
    tickers = metadata_df['ticker'].str.upper().tolist()

    all_metrics = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(get_latest_metrics, ticker): ticker for ticker in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                metrics_df = future.result()
                if not metrics_df.empty:
                    metrics_df['ticker'] = ticker
                    metadata_row = metadata_df[metadata_df['ticker'].str.upper() == ticker].iloc[0]
                    metrics_df['company_name'] = metadata_row.get('company_name', pd.NA)
                    metrics_df['sic_code'] = metadata_row.get('sic_code', pd.NA)
                    metrics_df['industry_name'] = metadata_row.get('industry_name', pd.NA)
                    metrics_df['sec_filing_url'] = metadata_row.get('sec_filing_url', pd.NA)
                    all_metrics.append(metrics_df)
            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
    return pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()

def fetch_market_data(tickers_df):
    """Fetch market data from Yahoo Finance for filing dates in parallel."""
    market_data = []
    tickers_df['filed'] = pd.to_datetime(tickers_df['filed'], errors='coerce')

    def fetch_single_ticker(row):
        ticker = row['ticker']
        company_name = row['company_name']
        filing_date = pd.to_datetime(row['filed']).date()
        logger.info(f"Fetching market data for {ticker} on {filing_date}...")
        try:
            stock = yf.Ticker(ticker)
            df_history = stock.history(start=filing_date, end=filing_date + timedelta(days=1))
            if df_history.empty:
                logger.warning(f"No market data for {ticker} on {filing_date}.")
                return []
            price = df_history['Close'].iloc[0]
            if pd.isna(price):
                logger.warning(f"Invalid price for {ticker} on {filing_date}.")
                return []
            market_cap = None
            for attempt in range(3):
                try:
                    info = stock.info
                    shares_outstanding = info.get('sharesOutstanding')
                    if shares_outstanding:
                        market_cap = price * float(shares_outstanding)
                    else:
                        market_cap = info.get('marketCap')
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt+1} failed for {ticker}: {e}")
                    time.sleep(2)
            return [
                {
                    'ticker': ticker, 'item': 'Market_Price', 'val': float(price),
                    'date': filing_date.isoformat(), 'filed': pd.to_datetime(row['filed']),
                    'company_name': company_name, 'fy': None, 'fp': None, 'form': None,
                    'frame': None, 'start': None, 'end': None, 'sic_code': '',
                    'industry_name': '', 'sec_filing_url': ''
                },
                {
                    'ticker': ticker, 'item': 'Market_Cap', 'val': float(market_cap) if market_cap else np.nan,
                    'date': filing_date.isoformat(), 'filed': pd.to_datetime(row['filed']),
                    'company_name': company_name, 'fy': None, 'fp': None, 'form': None,
                    'frame': None, 'start': None, 'end': None, 'sic_code': '',
                    'industry_name': '', 'sec_filing_url': ''
                }
            ]
        except Exception as e:
            logger.error(f"Error fetching market data for {ticker}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_row = {executor.submit(fetch_single_ticker, row): row for _, row in tickers_df.iterrows()}
        for future in as_completed(future_to_row):
            market_data.extend(future.result())
    return market_data

def main(event=None, context=None):
    # Load credentials
    key_path = "/home/eraphaelparra/aialchemy.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    client = bigquery.Client(project='aialchemy', credentials=credentials)
    logger.info(f"Using credentials from {key_path}")

    # Step 1: Fetch SEC data
    metadata_csv_path = "/home/eraphaelparra/profit-scout/data/sp500_metadata.csv"
    logger.info("Fetching SEC data for S&P 500 tickers.")
    sec_df = load_all_financial_metrics(metadata_csv_path)

    if not sec_df.empty:
        sec_df = sec_df.sort_values('filed').groupby('ticker').tail(1)
        sec_df['filed'] = pd.to_datetime(sec_df['filed'], errors='coerce')
        pandas_gbq.to_gbq(
            sec_df, 'aialchemy.financial_data.financial_metrics', project_id='aialchemy',
            if_exists='replace', credentials=credentials
        )
        logger.info(f"Uploaded {len(sec_df)} rows to financial_metrics.")
    else:
        logger.error("No SEC data fetched.")
        return

    # Step 2: Fetch market data
    tickers_df = sec_df[['ticker', 'company_name', 'filed']].drop_duplicates()
    market_data = fetch_market_data(tickers_df)

    if market_data:
        market_df = pd.DataFrame(market_data)
        market_df['filed'] = pd.to_datetime(market_df['filed'], errors='coerce')
        market_df['val'] = pd.to_numeric(market_df['val'], errors='coerce')
        for col in market_df.columns:
            if market_df[col].dtype == 'object' and col != 'filed':
                market_df[col] = market_df[col].astype(str)
        pandas_gbq.to_gbq(
            market_df, 'aialchemy.financial_data.financial_metrics', project_id='aialchemy',
            if_exists='append', credentials=credentials
        )
        logger.info(f"Appended {len(market_df)} market data rows.")
    else:
        logger.error("No market data fetched.")
        return

    # Step 3: Calculate ratios
    query = "SELECT company_name, ticker, item, val, filed FROM `aialchemy.financial_data.financial_metrics`"
    df = client.query(query).to_dataframe()
    df['filed'] = pd.to_datetime(df['filed'], errors='coerce')
    df['val'] = pd.to_numeric(df['val'], errors='coerce')
    metrics_pivot = df.pivot_table(
        index=['company_name', 'ticker', 'filed'], columns='item', values='val', aggfunc='max'
    ).reset_index()
    metrics_pivot.columns = [col if isinstance(col, str) else col[1] for col in metrics_pivot.columns]

    standard_mapping = {
        "Net Income": ["Net Income (Loss) Attributable to Parent"],
        "Total Equity": ["Stockholders' Equity Attributable to Parent"],
        "Total Debt": ["Liabilities"],
        "Current Assets": ["Assets, Current"],
        "Current Liabilities": ["Liabilities, Current"],
        "Revenues": ["Revenue from Contract with Customer, Excluding Assessed Tax", "Revenues"],
        "Cost of Goods Sold": ["Cost of Goods and Services Sold"],
        "Earnings Per Share": ["Earnings Per Share, Basic"],
        "Market Price": ["Market_Price"],
        "Operating Cash Flow": ["Net Cash Provided by (Used in) Operating Activities"],
        "Payments to Acquire Property, Plant, and Equipment": ["Payments to Acquire Property, Plant, and Equipment"],
        "Market Cap": ["Market_Cap"]
    }

    for standard_name, possible_names in standard_mapping.items():
        available_columns = [col for col in possible_names if col in metrics_pivot.columns]
        if available_columns:
            metrics_pivot[standard_name] = metrics_pivot[available_columns].bfill(axis=1).iloc[:, 0]
        else:
            metrics_pivot[standard_name] = np.nan

    metrics_pivot['ROE'] = metrics_pivot['Net Income'] / metrics_pivot['Total Equity']
    metrics_pivot['Debt_to_Equity'] = metrics_pivot['Total Debt'] / metrics_pivot['Total Equity']
    metrics_pivot['Current_Ratio'] = metrics_pivot['Current Assets'] / metrics_pivot['Current Liabilities']
    metrics_pivot['Gross_Margin'] = (metrics_pivot['Revenues'] - metrics_pivot.get('Cost of Goods Sold', 0)) / metrics_pivot['Revenues']
    metrics_pivot['Free_Cash_Flow'] = metrics_pivot['Operating Cash Flow'] - metrics_pivot.get('Payments to Acquire Property, Plant, and Equipment', 0)
    metrics_pivot['P_E_Ratio'] = metrics_pivot['Market Price'] / metrics_pivot['Earnings Per Share']
    metrics_pivot['FCF_Yield'] = metrics_pivot['Free_Cash_Flow'] / metrics_pivot['Market Cap']

    ratios_df = metrics_pivot.melt(
        id_vars=['company_name', 'ticker', 'filed'],
        value_vars=['ROE', 'Debt_to_Equity', 'Current_Ratio', 'Gross_Margin', 'P_E_Ratio', 'FCF_Yield'],
        var_name='Ratio', value_name='Value'
    ).dropna(subset=['Value'])
    ratios_df.rename(columns={'company_name': 'Company_Name', 'ticker': 'Ticker'}, inplace=True)
    ratios_df['filed'] = pd.to_datetime(ratios_df['filed'], errors='coerce')

    pandas_gbq.to_gbq(
        ratios_df, 'aialchemy.financial_data.financial_ratios', project_id='aialchemy',
        if_exists='replace', credentials=credentials
    )
    logger.info(f"Created financial_ratios with {len(ratios_df)} rows.")
    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    main()