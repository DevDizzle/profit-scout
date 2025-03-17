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
MAX_DAYS_FORWARD = 5      # how many days forward we check from the reporting end date

logger = logging.getLogger('financial_pipeline')
logger.setLevel(logging.DEBUG)  # set DEBUG for detailed logging
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

# =============================================================================
#             GLOBAL SETTINGS & CIK MAPPING FOR TARGET STOCKS
# =============================================================================

# For our six stocks, we define the CIKs explicitly.
CIK_MAPPING = {
    "CTVA": "0001755672",
    "AES":  "0000874761",
    "NTAP": "0001002047",
    "GIS":  "0000040704",
    "NOC":  "0001133421",
    "CMI":  "0000026172"
}

# =============================================================================
#                     HELPER FUNCTIONS: SEC DATA & REPORTING DATE
# =============================================================================

def _facts_url(cik: str) -> str:
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"

def _submissions_url(cik: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_submission_data_for_ticker(ticker: str) -> pd.DataFrame:
    """
    Fetch recent submission data (e.g., 10-K/10-Q filings) for a given ticker.
    """
    cik = CIK_MAPPING.get(ticker.upper())
    if not cik:
        raise ValueError(f"No CIK for ticker {ticker}")
    url = _submissions_url(cik)
    logger.debug(f"Fetching submission data for {ticker} from {url}")
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    data = resp.json().get("filings", {}).get("recent", {})
    df = pd.DataFrame(data)
    logger.debug(f"Retrieved {df.shape[0]} submission rows for {ticker}")
    return df

def get_latest_filing_end_date(ticker: str) -> pd.Timestamp:
    """
    For the given ticker, get the official reporting period end date from the latest 10-K/10-Q.
    We expect the submission data to include a field such as 'reportPeriod' (or 'periodOfReport').
    """
    sub_df = get_submission_data_for_ticker(ticker)
    if sub_df.empty:
        logger.warning(f"No submission data for {ticker}.")
        return pd.NaT

    # Filter to filings that are 10-K or 10-Q
    sub_df = sub_df[sub_df['form'].isin(['10-K','10-Q'])].copy()
    if sub_df.empty:
        logger.warning(f"No 10-K or 10-Q filings for {ticker}.")
        return pd.NaT

    # Convert filingDate to datetime
    sub_df['filingDate'] = pd.to_datetime(sub_df['filingDate'], errors='coerce')
    # Sort descending by filingDate
    sub_df = sub_df.sort_values('filingDate', ascending=False)
    
    # Try to extract the reporting period end date.
    # Look for a field such as 'reportPeriod' or 'periodOfReport'
    possible_cols = ['reportPeriod', 'periodOfReport', 'reportdate']
    lower_cols = [c.lower() for c in sub_df.columns]
    found_col = None
    for c in possible_cols:
        if c.lower() in lower_cols:
            found_col = c
            break

    if not found_col:
        logger.warning(f"No official reporting date field found in submission data for {ticker}.")
        return pd.NaT

    latest_row = sub_df.iloc[0]
    date_str = latest_row.get(found_col, "")
    end_date = pd.to_datetime(date_str, errors='coerce')
    if pd.isna(end_date):
        logger.warning(f"Could not parse a valid date from {found_col} for {ticker}")
    else:
        logger.debug(f"For {ticker}, official reporting end date is {end_date}")
    return end_date

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_facts_json(ticker: str) -> dict:
    """
    Fetch the XBRL company facts JSON for the given ticker.
    """
    cik = CIK_MAPPING.get(ticker.upper())
    if not cik:
        raise ValueError(f"No CIK for ticker {ticker}")
    url = _facts_url(cik)
    logger.debug(f"Fetching facts JSON for {ticker} from {url}")
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    return resp.json()

def facts_to_df(facts_json: dict) -> pd.DataFrame:
    """
    Convert the 'us-gaap' portion of the XBRL facts into a DataFrame.
    """
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

def get_latest_filing_facts(ticker: str) -> pd.DataFrame:
    """
    Get the SEC facts for the given ticker, filtering only to the rows
    that correspond to the official reporting period end date.
    """
    facts_json = get_facts_json(ticker)
    df_facts = facts_to_df(facts_json)
    if df_facts.empty:
        return pd.DataFrame()
    df_facts['end'] = pd.to_datetime(df_facts['end'], errors='coerce')
    actual_end_date = get_latest_filing_end_date(ticker)
    if pd.isna(actual_end_date):
        logger.warning(f"{ticker}: No official reporting end date found; using fallback max(end).")
        last_date = df_facts['end'].max()
    else:
        if actual_end_date in df_facts['end'].values:
            last_date = actual_end_date
        else:
            logger.warning(f"{ticker}: Official reporting end date {actual_end_date} not found in facts; using fallback.")
            last_date = df_facts['end'].max()
    filtered = df_facts[df_facts['end'] == last_date].copy()
    logger.debug(f"{ticker} facts for reporting period end {last_date.date()} sample:\n{filtered.head()}")
    return filtered

# =============================================================================
#                     FETCH MARKET DATA (SERIAL)
# =============================================================================

def fetch_single_ticker_market_data(ticker: str, reported_date: pd.Timestamp) -> dict:
    """
    For the given reporting date, try up to MAX_DAYS_FORWARD to find the first trading day
    with market data. Returns a dictionary with market price, market cap, etc.
    """
    if pd.isna(reported_date):
        return {}
    base_date = reported_date.date() if hasattr(reported_date, 'date') else reported_date
    for offset in range(MAX_DAYS_FORWARD + 1):
        try:
            try_date = base_date + timedelta(days=offset)
            if try_date.weekday() >= 5:
                continue
            time.sleep(random.uniform(*SLEEP_TIME))
            stock = yf.Ticker(ticker)
            hist = stock.history(start=try_date, end=try_date + timedelta(days=1), interval='1d')
            if hist.empty:
                continue
            close_price = hist['Close'].iloc[0]
            if pd.isna(close_price):
                continue
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
            logger.debug(f"{ticker} market data on {try_date}: Price={close_price}, Cap={market_cap}")
            return {
                'ticker': ticker,
                'as_of_date': str(base_date),  # reporting period end date
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
#                     CALCULATE RATIOS & FINAL OUTPUT
# =============================================================================

def calculate_ratios(sec_df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the SEC facts, merge with market data, and calculate key financial ratios.
    """
    if sec_df.empty:
        logger.warning("SEC DataFrame is empty! No ratios to compute.")
        return pd.DataFrame()
    if market_df.empty:
        logger.warning("Market DataFrame is empty! No ratios to compute.")
        return pd.DataFrame()
    logger.info("Pivoting SEC data to compute ratios...")
    # Fill missing grouping columns
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
    # Use the reporting period as a string for merging market data
    pivoted['as_of_date'] = pivoted['end'].dt.date.astype(str)
    
    logger.debug("Pivoted SEC data columns:")
    logger.debug(pivoted.columns.tolist())
    
    # Standard mapping for the metrics we want to compute
    standard_map = {
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
        "Total Debt": [
            "LongTermDebt",
            "ShortTermDebt",
            "DebtAndCapitalLeaseObligations",
            "TotalDebt"
        ],
        "Current Assets": [
            "AssetsCurrent",
            "CurrentAssets"
        ],
        "Current Liabilities": [
            "LiabilitiesCurrent",
            "CurrentLiabilities"
        ],
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
        "Earnings Per Share": [
            "EarningsPerShareBasic",
            "EarningsPerShareDiluted",
            "EarningsPerShare",
            "BasicEarningsPerShare",
            "DilutedEarningsPerShare"
        ],
        "Operating Cash Flow": [
            "NetCashProvidedByUsedInOperatingActivities",
            "OperatingCashFlow",
            "CashFlowFromOperations",
            "NetCashFromOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
        ],
        "CapEx": [
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "CapitalExpendituresIncurredButNotYetPaid",
            "CapitalExpenditures",
            "CapitalExpenditure",
            "PurchaseOfPropertyPlantAndEquipment",
            "InvestingCashFlowCapitalExpenditures",
            "AcquisitionsNet"
        ]
    }

    for metric, synonyms in standard_map.items():
        found_cols = [col for col in synonyms if col in pivoted.columns]
        if found_cols:
            pivoted[metric] = pivoted[found_cols].bfill(axis=1).iloc[:, 0]
        else:
            pivoted[metric] = np.nan
            logger.debug(f"For metric '{metric}', no matching columns found among {synonyms}")
    
    logger.info(f"Merging pivoted SEC data (shape={pivoted.shape}) with market data (shape={market_df.shape})")
    merged = pd.merge(
        pivoted,
        market_df,
        how='left',
        on=['ticker', 'as_of_date']
    )
    
    logger.debug("Merged data sample:")
    logger.debug(merged.head(10).to_string())
    
    if merged.empty:
        logger.warning("Final merge returned an empty DataFrame. No matching (ticker, as_of_date).")
        return pd.DataFrame()
    
    # Calculate ratios
    merged['ROE'] = pd.to_numeric(merged['Net Income'], errors='coerce') / pd.to_numeric(merged['Total Equity'], errors='coerce')
    merged['Debt_to_Equity'] = pd.to_numeric(merged['Total Debt'], errors='coerce') / pd.to_numeric(merged['Total Equity'], errors='coerce')
    merged['Current_Ratio'] = pd.to_numeric(merged['Current Assets'], errors='coerce') / pd.to_numeric(merged['Current Liabilities'], errors='coerce')
    merged['Gross_Margin'] = (pd.to_numeric(merged['Revenues'], errors='coerce') - pd.to_numeric(merged.get('Cost of Goods Sold', 0.0), errors='coerce')) / pd.to_numeric(merged['Revenues'], errors='coerce')
    merged['Free_Cash_Flow'] = pd.to_numeric(merged['Operating Cash Flow'], errors='coerce') - pd.to_numeric(merged.get('CapEx', 0.0), errors='coerce')
    merged['P_E_Ratio'] = pd.to_numeric(merged['market_price'], errors='coerce') / pd.to_numeric(merged['Earnings Per Share'], errors='coerce')
    merged['FCF_Yield'] = pd.to_numeric(merged['Free_Cash_Flow'], errors='coerce') / pd.to_numeric(merged['market_cap'], errors='coerce')
    
    logger.debug("Calculated ratios sample:")
    logger.debug(merged[['ticker', 'ROE', 'Debt_to_Equity', 'Current_Ratio', 'Gross_Margin', 'P_E_Ratio', 'FCF_Yield']].head(10).to_string())
    
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
    Complete pipeline for six target stocks with updated reporting date logic:
      1. Load metadata (for our six stocks).
      2. Build CIK mapping.
      3. For each ticker, fetch SEC facts and filter to the official reporting period end date.
      4. Fetch market data using that reporting date.
      5. Pivot, merge, and calculate ratios.
      6. Upload the final table to BigQuery.
    """
    # 1) Credentials
    key_path = "/home/eraphaelparra/aialchemy.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    client = bigquery.Client(project='aialchemy', credentials=credentials)
    logger.info(f"Using credentials from {key_path}")

    # 2) Load metadata CSV (ensure it has 'ticker', 'company_name', 'industry_name', 'segment', and 'cik')
    metadata_csv_path = "/home/eraphaelparra/profit-scout/data/sp500_metadata.csv"
    meta_df = pd.read_csv(metadata_csv_path)
    target_tickers = ["CTVA", "AES", "NTAP", "GIS", "NOC", "CMI"]
    meta_df = meta_df[meta_df['ticker'].str.upper().isin(target_tickers)]
    logger.info(f"Filtered metadata to {len(meta_df)} companies: {target_tickers}")

    # Global CIK_MAPPING is already defined above.

    # 3) Loop through tickers to fetch SEC facts (using reporting end date) and attach metadata
    sec_frames = []
    tickers = meta_df['ticker'].str.upper().unique().tolist()
    logger.info(f"Fetching SEC metrics for {len(tickers)} tickers...")
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"[{i}/{len(tickers)}] Processing ticker: {ticker}")
        try:
            df_facts = get_latest_filing_facts(ticker)
            if df_facts.empty:
                logger.info(f"No SEC data for {ticker}. Skipping.")
                continue
            logger.debug(f"Raw SEC data for {ticker} (filtered to reporting period):\n{df_facts.head()}")
            df_facts["ticker"] = ticker
            row = meta_df[meta_df['ticker'].str.upper() == ticker].iloc[0]
            df_facts["company_name"] = row.get("company_name", pd.NA)
            df_facts["industry_name"] = row.get("industry_name", pd.NA)
            df_facts["segment"] = row.get("segment", pd.NA)
            sec_frames.append(df_facts)
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {e}")

    if not sec_frames:
        logger.error("No SEC data collected for any ticker. Exiting.")
        return
    sec_df = pd.concat(sec_frames, ignore_index=True)
    logger.info(f"Combined SEC DataFrame shape: {sec_df.shape}")
    logger.debug("Combined SEC DataFrame sample:")
    print(sec_df.head(10).to_string())

    # 4) Loop through unique (ticker, end) pairs to fetch market data
    market_frames = []
    needed = sec_df[['ticker', 'end']].drop_duplicates().dropna(subset=['ticker', 'end'])
    total = len(needed)
    logger.info(f"Fetching market data for {total} (ticker, end) pairs...")
    for count, row in needed.iterrows():
        ticker = row['ticker']
        end_date = row['end']
        logger.info(f"Fetching market data for {ticker} (reporting end={end_date.date()})...")
        data_dict = fetch_single_ticker_market_data(ticker, end_date)
        if data_dict:
            logger.info(f"  => Found market data for {ticker} on {data_dict['actual_market_date']}")
            market_frames.append(data_dict)
        else:
            logger.info(f"  => No market data found for {ticker} near {end_date.date()}")
    if not market_frames:
        logger.error("No market data fetched. Exiting.")
        return
    market_df = pd.DataFrame(market_frames)
    logger.info(f"Market DataFrame shape: {market_df.shape}")
    logger.debug("Market DataFrame sample:")
    print(market_df.head(10).to_string())

    # 5) Calculate ratios
    final_df = calculate_ratios(sec_df, market_df)
    if final_df.empty:
        logger.warning("Final DataFrame is empty; nothing to upload. Exiting.")
        return
    logger.info(f"Final table shape: {final_df.shape}")
    logger.debug("Final DataFrame sample:")
    print(final_df.head(10).to_string())

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
