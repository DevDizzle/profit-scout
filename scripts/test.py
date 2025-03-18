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

# =============================================================================
#                      STANDARD FACT MAPPING CONFIGURATION
# =============================================================================

standard_map = {
    # For ROE: Net Income / Total Equity
    "Net Income": [
        "NetIncomeLossAvailableToCommonStockholdersBasic", 
        "NetIncomeFromContinuingOperations",               
        "NetIncomeApplicableToCommonStockholders",           
        "NetIncomeLoss",                                     
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
        "TotalDebt",                                   
        "LongTermDebtAndCapitalLeaseObligations",      
        "DebtAndCapitalLeaseObligations",             
        "LongTermDebt",                                
        "DebtCurrent",                                
        "LongTermDebtCurrent",                         
        "ShortTermDebt",                               
        "DebtInstrumentUnamortizedDiscountPremiumAndDebtIssuanceCostsNet"  
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
        "PaymentsToAcquireProductiveAssets",             
        "PaymentsToAcquirePropertyPlantAndEquipment",    
        "CapitalExpendituresIncurredButNotYetPaid",
        "CapitalExpenditures",
        "CapitalExpenditure",
        "PurchaseOfPropertyPlantAndEquipment",
        "InvestingCashFlowCapitalExpenditures",
        "AcquisitionsNet"
    ]
}
# =============================================================================
#                   HELPER FUNCTIONS: SEC DATA & CIK MAPPING
# =============================================================================

def _submission_url(cik: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"

def _facts_url(cik: str) -> str:
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_submission_data_for_ticker(ticker: str) -> pd.DataFrame:
    """
    Fetch recent SEC submissions for a ticker (using its CIK).
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
    Return (accessionNumber, formType, filingDate) for the most recent 10-K/10-Q.
    """
    try:
        df = get_submission_data_for_ticker(ticker)
        relevant = df[df['form'].isin(['10-K', '10-Q'])].copy()
        if relevant.empty:
            logger.warning(f"No 10-K/10-Q found for {ticker}")
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
    Fetch the SEC XBRL facts JSON for a ticker.
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
    Convert the 'us-gaap' portion of the SEC XBRL facts JSON to a DataFrame.
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
    For a given ticker, this function:
      1. Retrieves the most recent 10-K/10-Q info (accession, form, filing date).
      2. Pulls the SEC facts JSON and converts it into a DataFrame.
      3. Converts the 'filed' and 'end' columns to datetime and excludes future 'end' dates.
      4. Filters to only include rows where the 'filed' date equals the official filing date.
      5. Restricts to allowed fact keys (as defined in standard_map).
      6. For each mapping (friendly name), selects the row with the maximum 'end' date.
         If multiple rows have that same end date, it selects the one with the latest 'start' date.
    The result is a DataFrame with one row per mapping.
    """
    accession, form_type, filing_date = get_latest_filing_info(ticker)
    if not accession:
        return pd.DataFrame()
    try:
        data_json = get_facts_json(ticker)
        df_facts = facts_to_df(data_json)
        if df_facts.empty:
            return pd.DataFrame()
        # Convert 'end' and 'filed' columns to datetime.
        df_facts['end'] = pd.to_datetime(df_facts['end'], errors='coerce')
        df_facts['filed'] = pd.to_datetime(df_facts['filed'], errors='coerce')
        today = pd.Timestamp.today().normalize()
        df_facts = df_facts[df_facts['end'] <= today]
        if df_facts.empty:
            logger.info(f"{ticker}: All 'end' dates are in the future, skipping.")
            return pd.DataFrame()
        # Filter to rows matching the official filing date (or fallback to max filed date).
        official_filing_date = pd.to_datetime(filing_date)
        df_official = df_facts[df_facts['filed'] == official_filing_date]
        if df_official.empty:
            logger.warning(f"{ticker}: No facts match official filing date {official_filing_date}. Using max filed date instead.")
            max_filed = df_facts['filed'].max()
            df_official = df_facts[df_facts['filed'] == max_filed]
        # Build set of allowed fact keys from the mapping.
        allowed_fact_keys = set()
        for keys in standard_map.values():
            allowed_fact_keys.update(keys)
        df_mapped = df_official[df_official['fact_key'].isin(allowed_fact_keys)].copy()
        # For each mapping, select the row with the maximum 'end' date,
        # using the latest 'start' date as tie-breaker.
        result_rows = []
        for friendly, keys in standard_map.items():
            sub = df_mapped[df_mapped['fact_key'].isin(keys)]
            if sub.empty:
                continue
            # Get the maximum 'end' date for these rows.
            max_end = sub['end'].max()
            sub_max_end = sub[sub['end'] == max_end]
            # Tie-breaker: sort by 'start' in descending order (later start means shorter period).
            # Note: if 'start' is NaT, it will be sorted last.
            sub_sorted = sub_max_end.sort_values(by='start', ascending=False)
            row_max = sub_sorted.iloc[0].copy()
            row_max['mapping'] = friendly
            result_rows.append(row_max)
        if not result_rows:
            return pd.DataFrame()
        df_result = pd.DataFrame(result_rows)
        # Add metadata columns.
        df_result['accn'] = accession
        df_result['form'] = form_type
        df_result['filed'] = filing_date
        df_result['ticker'] = ticker
        return df_result
    except Exception as exc:
        logger.error(f"Error processing SEC facts for {ticker}: {exc}")
        return pd.DataFrame()

# =============================================================================
#                     PIPELINE: FETCH MARKET DATA (SERIAL)
# =============================================================================

def fetch_single_ticker_market_data(ticker: str, reported_date: pd.Timestamp) -> dict:
    """
    For the given 'reported_date' (the official reporting period end date from SEC),
    try up to MAX_DAYS_FORWARD to locate the first trading day with data.
    Returns a dictionary with:
      [ticker, as_of_date, actual_market_date, market_price, market_cap, dividend].
    """
    if pd.isna(reported_date):
        return {}
    base_date = reported_date.date() if hasattr(reported_date, 'date') else reported_date
    for offset in range(MAX_DAYS_FORWARD + 1):
        try:
            try_date = base_date + timedelta(days=offset)
            if try_date.weekday() >= 5:
                continue  # skip weekends
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
            return {
                'ticker': ticker,
                'as_of_date': str(base_date),
                'actual_market_date': str(try_date),
                'market_price': float(close_price),
                'market_cap': float(market_cap) if market_cap else np.nan,
                'dividend': float(dividend)
            }
        except Exception as exc:
            logger.error(f"Error fetching market data for {ticker} on {try_date}: {exc}")
            continue
    return {}

# =============================================================================
#                     PIPELINE: CALCULATE RATIOS & FINAL OUTPUT
# =============================================================================

def calculate_ratios(sec_df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Pivot the SEC data (which now has a 'mapping' column) so that each ticker becomes one row
       with one column per mapping.
    2. Merge with market_df on (ticker, as_of_date).
    3. Compute financial ratios.
    """
    if sec_df.empty:
        logger.warning("SEC DataFrame is empty! No ratios to compute.")
        return pd.DataFrame()
    if market_df.empty:
        logger.warning("Market DataFrame is empty! No ratios to compute.")
        return pd.DataFrame()
    logger.info("Pivoting SEC data to compute ratios...")
    # Ensure metadata columns exist.
    sec_df['company_name'] = sec_df['company_name'].fillna('UnknownCompany')
    sec_df['industry_name'] = sec_df['industry_name'].fillna('UnknownIndustry')
    sec_df['segment'] = sec_df['segment'].fillna('UnknownSegment')
    pivot_cols = ['ticker', 'company_name', 'industry_name', 'segment', 'end']
    df_sec = sec_df[pivot_cols + ['mapping', 'value']].copy().dropna(subset=['ticker', 'end', 'mapping'])
    pivoted = df_sec.pivot_table(
        index=pivot_cols,
        columns='mapping',
        values='value',
        aggfunc='first'
    ).reset_index()
    # The as_of_date comes from the SEC reporting period end.
    pivoted['as_of_date'] = pivoted['end'].dt.date.astype(str)
    logger.info(f"Merging pivoted SEC data (shape={pivoted.shape}) with market data (shape={market_df.shape})")
    merged = pd.merge(
        pivoted,
        market_df,
        how='left',
        on=['ticker', 'as_of_date']
    )
    if merged.empty:
        logger.warning("Final merge returned an empty DataFrame.")
        return pd.DataFrame()
    # Calculate ratios.
    merged['ROE'] = merged['Net Income'] / merged['Total Equity']
    merged['Debt_to_Equity'] = merged['Total Debt'] / merged['Total Equity']
    merged['Current_Ratio'] = merged['Current Assets'] / merged['Current Liabilities']
    merged['Gross_Margin'] = (merged['Revenues'] - merged.get('Cost of Goods Sold', 0.0)) / merged['Revenues']
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
    Complete pipeline for all stocks (or our test set):
      1. Load credentials.
      2. Load metadata and build CIK mapping.
      3. For each ticker, fetch SEC data using updated mapping logic and attach metadata.
      4. For each ticker, fetch market data using the official reporting period end date.
      5. Calculate ratios.
      6. Upload the final table to BigQuery.
    """
    # 1) Credentials
    key_path = "/home/eraphaelparra/aialchemy.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    client = bigquery.Client(project='aialchemy', credentials=credentials)
    logger.info(f"Using credentials from {key_path}")

    # 2) For testing, define a small metadata DataFrame and explicit CIK mapping.
    global CIK_MAPPING
    CIK_MAPPING = {
        "CTVA": "0001755672",
        "AES":  "0000874761",
        "NTAP": "0001002047",
        "GIS":  "0000040704",
        "NOC":  "0001133421",
        "CMI":  "0000026172"
    }
    meta_data = [
        {"ticker": "CTVA", "company_name": "Corteva Inc.", "industry_name": "Agriculture", "segment": "Agribusiness"},
        {"ticker": "AES",  "company_name": "The AES Corporation", "industry_name": "Utilities", "segment": "Electric"},
        {"ticker": "NTAP", "company_name": "NetApp Inc.", "industry_name": "Technology", "segment": "Data Management"},
        {"ticker": "GIS",  "company_name": "General Mills Inc.", "industry_name": "Consumer Staples", "segment": "Food"},
        {"ticker": "NOC",  "company_name": "Northrop Grumman Corp.", "industry_name": "Defense", "segment": "Aerospace & Defense"},
        {"ticker": "CMI",  "company_name": "Cummins Inc.", "industry_name": "Industrial", "segment": "Power Systems"},
    ]
    meta_df = pd.DataFrame(meta_data)
    logger.info(f"Loaded metadata for {len(meta_df)} companies.")

    # 3) Loop through tickers to fetch SEC data and attach metadata.
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
            df_facts["ticker"] = ticker
            # Attach metadata.
            row = meta_df[meta_df['ticker'].str.upper() == ticker].iloc[0]
            df_facts["company_name"] = row.get("company_name", pd.NA)
            df_facts["industry_name"] = row.get("industry_name", pd.NA)
            df_facts["segment"] = row.get("segment", pd.NA)
            sec_frames.append(df_facts)
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {e}")
    if not sec_frames:
        logger.error("No SEC data collected. Exiting.")
        return
    sec_df = pd.concat(sec_frames, ignore_index=True)
    logger.info(f"Combined SEC DataFrame shape: {sec_df.shape}")

    # 4) Loop through unique (ticker, end) pairs to fetch market data.
    market_frames = []
    needed = sec_df[['ticker', 'end']].drop_duplicates().dropna(subset=['ticker', 'end'])
    total = len(needed)
    logger.info(f"Fetching market data for {total} (ticker, end) pairs...")
    for count, row in needed.iterrows():
        ticker = row['ticker']
        end_date = row['end']
        logger.info(f"Fetching market data for {ticker} (end={end_date.date()})...")
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

    # 5) Calculate ratios.
    final_df = calculate_ratios(sec_df, market_df)
    if final_df.empty:
        logger.warning("Final DataFrame is empty; nothing to upload. Exiting.")
        return
    logger.info(f"Final table shape: {final_df.shape}")

    # 6) Upload final table to BigQuery.
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
