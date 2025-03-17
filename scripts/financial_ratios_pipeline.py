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

# Global ticker -> padded CIK mapping. For AAPL, the CIK is 0000320193.
CIK_MAPPING = {"AAPL": "0000320193"}

# =============================================================================
#                     HELPER FUNCTIONS (SEC + METADATA)
# =============================================================================

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
    Extracts the fact key (as 'fact_key') and the reported value (as 'value')
    along with other metadata.
    """
    rows = []
    gaap_facts = facts_json.get('facts', {}).get('us-gaap', {})
    if not gaap_facts:
        logger.warning("No us-gaap facts found.")
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
                    'start': entry.get('start', None),
                    'end': entry.get('end', None),
                    'fy': entry.get('fy'),
                    'fp': entry.get('fp'),
                    'form': entry.get('form'),
                    'filed': entry.get('filed')
                })

    return pd.DataFrame(rows)

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
#                     PIPELINE: FETCH MARKET DATA (SERIAL)
# =============================================================================

def fetch_single_ticker_market_data(ticker: str, reported_date: pd.Timestamp) -> dict:
    """
    For the given 'reported_date' (the last reported 'end' from SEC),
    we try up to MAX_DAYS_FORWARD to find the first trading day with data.
    Return a single dict with:
      [ticker, as_of_date, actual_market_date, market_price, market_cap, dividend].
    """
    if pd.isna(reported_date):
        return {}

    base_date = reported_date.date() if hasattr(reported_date, 'date') else reported_date

    for offset in range(MAX_DAYS_FORWARD + 1):
        try_date = base_date + timedelta(days=offset)
        # Skip weekends quickly
        if try_date.weekday() >= 5:
            continue

        # Polite, random sleep
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

# =============================================================================
#                     PIPELINE: CALCULATE RATIOS & FINAL OUTPUT
# =============================================================================

def calculate_ratios(sec_df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    """
    1) Pivot SEC data on (ticker, company_name, industry_name, segment, end).
    2) Merge with market_df on (ticker, as_of_date).
    3) Calculate ratios.
    4) Return final DataFrame with extra columns: industry_name, segment, etc.
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
    df_sec = sec_df[pivot_cols + ['fact_key', 'value']].copy().dropna(subset=['ticker', 'end', 'fact_key'])

    pivoted = df_sec.pivot_table(
        index=pivot_cols,
        columns='fact_key',
        values='value',
        aggfunc='max'
    ).reset_index()

    pivoted['as_of_date'] = pivoted['end'].dt.date.astype(str)

    standard_map = {
        # For ROE: Net Income / Total Equity
        "Net Income": [
            "NetIncomeLoss",
            "NetIncomeFromContinuingOperations",
            "NetIncomeApplicableToCommonStockholders",
            "NetIncome"
        ],
        "Total Equity": [
            "StockholdersEquity",
            "StockholdersEquityAttributableToParent",
            "TotalStockholdersEquity",
            "ShareholdersEquity",
            "CommonStockholdersEquity"
        ],
        # For Debt-to-Equity: Total Debt / Total Equity
        "Total Debt": [
            "LongTermDebt",
            "ShortTermDebt",
            "DebtAndCapitalLeaseObligations",
            "TotalDebt"
        ],
        # For Current Ratio: Current Assets / Current Liabilities
        "Current Assets": [
            "AssetsCurrent",
            "CurrentAssets"
        ],
        "Current Liabilities": [
            "LiabilitiesCurrent",
            "CurrentLiabilities"
        ],
        # For Gross Margin: (Revenues - Cost of Goods Sold) / Revenues
        "Revenues": [
            "Revenues",
            "SalesRevenueNet",
            "RevenuesNetOfInterestExpense",
            "TotalRevenue",
            "Revenue",
            "RevenueFromContractWithCustomerExcludingAssessedTax"
        ],
        "Cost of Goods Sold": [
            "CostOfGoodsSold",
            "CostOfRevenue",
            "CostOfGoodsAndServicesSold"
        ],
        # For P/E Ratio: Market Price / Earnings Per Share
        "Earnings Per Share": [
            "EarningsPerShareBasic",
            "EarningsPerShareDiluted",
            "EarningsPerShare",
            "BasicEarningsPerShare",
            "DilutedEarningsPerShare"
        ],
        # For FCF Yield: (Operating Cash Flow - CapEx) / Market Cap
        "Operating Cash Flow": [
            "NetCashProvidedByUsedInOperatingActivities",
            "OperatingCashFlow",
            "CashFlowFromOperations",
            "NetCashFromOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
        ],
        "CapEx": [
            "PaymentsToAcquirePropertyPlantEquipment",
            "CapitalExpenditures",
            "CapitalExpenditure",
            "PurchaseOfPropertyPlantAndEquipment",
            "InvestingCashFlowCapitalExpenditures",
            "AcquisitionsNet"
        ]
    }

    for std_item, synonyms in standard_map.items():
        found_cols = [col for col in synonyms if col in pivoted.columns]
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
    Complete pipeline for a single stock (AAPL):
      1. Load GCP credentials.
      2. Manually set metadata for AAPL.
      3. Fetch SEC data for AAPL.
      4. Fetch Yahoo market data.
      5. Calculate ratios.
      6. Upload final table to BigQuery.
    """
    # 1) Credentials
    key_path = "/home/eraphaelparra/aialchemy.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    client = bigquery.Client(project='aialchemy', credentials=credentials)
    logger.info(f"Using credentials from {key_path}")

    # 2) Define the single ticker and its metadata
    ticker = "AAPL"
    # CIK_MAPPING already contains the mapping for AAPL.
    meta_df = pd.DataFrame({
        "ticker": [ticker],
        "company_name": ["Apple Inc."],
        "industry_name": ["Technology"],
        "segment": ["Consumer Electronics"]
    })

    # 3) Fetch SEC data for AAPL
    logger.info(f"Fetching SEC metrics for {ticker}...")
    df_facts = get_latest_filing_facts(ticker)
    if df_facts.empty:
        logger.error("No SEC data returned for AAPL. Exiting.")
        return
    df_facts["ticker"] = ticker
    # Attach metadata from the meta_df
    row = meta_df.iloc[0]
    df_facts["company_name"] = row.get("company_name", pd.NA)
    df_facts["industry_name"] = row.get("industry_name", pd.NA)
    df_facts["segment"] = row.get("segment", pd.NA)
    logger.info(f"SEC DataFrame shape: {df_facts.shape}")

    # 4) Fetch market data using the latest reported 'end' date from SEC facts
    reported_date = df_facts['end'].max()
    market_data = fetch_single_ticker_market_data(ticker, reported_date)
    if not market_data:
        logger.error("No market data fetched for AAPL. Exiting.")
        return
    market_df = pd.DataFrame([market_data])
    logger.info(f"Market DataFrame shape: {market_df.shape}")

    # 5) Calculate ratios
    final_df = calculate_ratios(df_facts, market_df)
    if final_df.empty:
        logger.warning("Final DataFrame is empty; nothing to upload. Exiting.")
        return
    logger.info(f"Final table shape: {final_df.shape}")

    # 6) Upload final table to BigQuery
    table_id = "aialchemy.financial_data.financial_ratios_final_aapl"
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
