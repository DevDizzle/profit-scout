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
MAX_DAYS_FORWARD = 5      # days forward to check for market data

# Set test ticker (change as needed)
TEST_TICKER = "AAPL"

# Output file for filtered raw SEC data
OUTPUT_CSV = "/home/eraphaelparra/profit-scout/data/filtered_sec_data.csv"

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
    cik_map = {str(row['ticker']).upper(): str(row['cik']).zfill(10)
               for _, row in df.iterrows()}
    logger.info(f"Loaded CIK mapping for {len(cik_map)} tickers.")
    return cik_map

def _submission_url(cik: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"

def _facts_url(cik: str) -> str:
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_submission_data_for_ticker(ticker: str) -> pd.DataFrame:
    """
    Fetch recent SEC submissions for a single ticker (using its CIK).
    """
    cik = CIK_MAPPING.get(ticker)
    if not cik:
        raise ValueError(f"No CIK for ticker {ticker}")
    url = _submission_url(cik)
    logger.info(f"Fetching submission data for {ticker} from {url}")
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
        logger.info(f"Latest filing for {ticker}: {recent['form']} on {recent['filingDate']}")
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
    logger.info(f"Fetching facts JSON for {ticker} from {url}")
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    return resp.json()

def facts_to_df(facts_json: dict) -> pd.DataFrame:
    """
    Convert the 'us-gaap' portion of the XBRL facts JSON into a DataFrame.
    Extracts fact_key, label, unit, value, start, end, fy, fp, form, and filed.
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
    df = pd.DataFrame(rows)
    logger.info(f"Created facts DataFrame with shape {df.shape}")
    return df

def get_latest_filing_facts(ticker: str) -> pd.DataFrame:
    """
    Fetch all SEC facts for the most recent 10-K/10-Q filing for the ticker.
    Only rows with an 'end' date in the past are retained.
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
        today = pd.Timestamp.today().normalize()
        df_facts = df_facts[df_facts['end'] <= today]
        if df_facts.empty:
            logger.info(f"{ticker}: All 'end' dates are in the future, skipping.")
            return pd.DataFrame()
        # Do not filter to a maximum 'end' date; return all rows.
        df_facts['accn'] = accession
        df_facts['form'] = form_type
        df_facts['filed'] = filing_date
        return df_facts
    except Exception as exc:
        logger.error(f"Error fetching facts for {ticker}: {exc}")
        return pd.DataFrame()

def load_raw_sec_data(metadata_csv: str) -> pd.DataFrame:
    """
    Load metadata, process only the TEST_TICKER, fetch all raw SEC facts,
    and return the combined DataFrame.
    """
    global CIK_MAPPING
    CIK_MAPPING = load_cik_mapping(metadata_csv)
    meta_df = pd.read_csv(metadata_csv)
    meta_df = meta_df[meta_df['ticker'].str.upper() == TEST_TICKER]
    tickers = meta_df['ticker'].str.upper().unique().tolist()
    logger.info(f"Fetching SEC metrics for {len(tickers)} ticker: {tickers}")
    all_frames = []
    for ticker in tickers:
        df_facts = get_latest_filing_facts(ticker)
        if df_facts.empty:
            logger.info(f"No valid facts found for {ticker}. Skipping.")
            continue
        df_facts['ticker'] = ticker
        row = meta_df[meta_df['ticker'].str.upper() == ticker].iloc[0]
        df_facts['company_name'] = row.get('company_name', pd.NA)
        df_facts['industry_name'] = row.get('industry_name', pd.NA)
        df_facts['segment'] = row.get('segment', pd.NA)
        all_frames.append(df_facts)
    if not all_frames:
        logger.info("No SEC data collected.")
        return pd.DataFrame()
    result = pd.concat(all_frames, ignore_index=True)
    logger.info(f"Combined SEC DataFrame shape: {result.shape}")
    return result

# =============================================================================
#                  FILTER RELEVANT FACT_KEYS AND EXPORT CSV
# =============================================================================

def export_filtered_sec_data(raw_df: pd.DataFrame, output_csv: str):
    """
    Filter the raw SEC DataFrame to include only rows with fact_key in the 
    comprehensive set of synonyms for the 6 ratio components and export to CSV.
    """
    # Comprehensive set of fact_key synonyms
    relevant_keys = {
        # Net Income synonyms (for ROE numerator)
        "NetIncomeLoss", "NetIncomeFromContinuingOperations", "NetIncomeApplicableToCommonStockholders", "NetIncome",
        # Total Equity synonyms (for ROE denominator)
        "StockholdersEquity", "StockholdersEquityAttributableToParent", "TotalStockholdersEquity", 
        "ShareholdersEquity", "CommonStockholdersEquity",
        # Total Debt synonyms (for Debt-to-Equity numerator)
        "LongTermDebt", "ShortTermDebt", "DebtAndCapitalLeaseObligations", "TotalDebt",
        # Current Assets synonyms (for Current Ratio numerator)
        "AssetsCurrent", "CurrentAssets",
        # Current Liabilities synonyms (for Current Ratio denominator)
        "LiabilitiesCurrent", "CurrentLiabilities",
        # Revenues synonyms (for Gross Margin numerator)
        "Revenues", "SalesRevenueNet", "RevenuesNetOfInterestExpense", "TotalRevenue", "Revenue", 
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        # Cost of Goods Sold synonyms (for Gross Margin denominator adjustment)
        "CostOfGoodsSold", "CostOfRevenue", "CostOfGoodsAndServicesSold",
        # Earnings Per Share synonyms (for P/E ratio denominator)
        "EarningsPerShareBasic", "EarningsPerShareDiluted", "EarningsPerShare", "BasicEarningsPerShare", "DilutedEarningsPerShare",
        # Operating Cash Flow synonyms (for FCF numerator)
        "NetCashProvidedByUsedInOperatingActivities", "OperatingCashFlow", "CashFlowFromOperations",
        "NetCashFromOperatingActivities", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        # CapEx synonyms (for FCF numerator subtraction)
        "PaymentsToAcquirePropertyPlantEquipment", "CapitalExpenditures", "CapitalExpenditure",
        "PurchaseOfPropertyPlantAndEquipment", "InvestingCashFlowCapitalExpenditures", "AcquisitionsNet"
    }
    filtered_df = raw_df[raw_df["fact_key"].isin(relevant_keys)].copy()
    logger.info(f"Filtered SEC data shape: {filtered_df.shape}")
    filtered_df.to_csv(output_csv, index=False)
    logger.info(f"Filtered SEC data exported to: {output_csv}")

# =============================================================================
#                                 MAIN
# =============================================================================

def main():
    """
    Load raw SEC data for the TEST_TICKER and export filtered raw facts to CSV.
    """
    metadata_csv_path = "/home/eraphaelparra/profit-scout/data/sp500_metadata.csv"
    raw_sec_df = load_raw_sec_data(metadata_csv_path)
    if raw_sec_df.empty:
        logger.error("No raw SEC data collected. Exiting.")
        return
    export_filtered_sec_data(raw_sec_df, OUTPUT_CSV)
    logger.info("Export of filtered SEC data complete.")

if __name__ == "__main__":
    main()
