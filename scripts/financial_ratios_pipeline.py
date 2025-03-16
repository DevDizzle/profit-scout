#!/usr/bin/env python3

import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from tenacity import retry, wait_exponential, stop_after_attempt
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq

# =============================================================================
#                           CONFIG & LOGGING
# =============================================================================

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

CIK_MAPPING = {}

# =============================================================================
#                      HELPER FUNCTIONS (SEC + METADATA)
# =============================================================================

def load_cik_mapping(metadata_csv: str) -> dict:
    df = pd.read_csv(metadata_csv)
    return {str(row['ticker']).upper(): str(row['cik']).zfill(10) for _, row in df.iterrows()}

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_facts_json(cik: str) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    resp = requests.get(url, headers=SEC_HEADERS)
    resp.raise_for_status()
    return resp.json()

def facts_to_df(facts_json: dict, ticker: str) -> pd.DataFrame:
    rows = []
    gaap_facts = facts_json.get('facts', {}).get('us-gaap', {})

    for fact_key, fact_val in gaap_facts.items():
        for unit, entries in fact_val.get('units', {}).items():
            for entry in entries:
                end_date = entry.get('end', None)
                if end_date:
                    rows.append({
                        'ticker': ticker,
                        'fact_key': fact_key,
                        'label': fact_val.get('label'),
                        'unit': unit,
                        'value': entry.get('val'),
                        'start': entry.get('start', None),
                        'end': end_date,
                        'fy': entry.get('fy'),
                        'fp': entry.get('fp'),
                        'form': entry.get('form'),
                        'filed': entry.get('filed')
                    })

    return pd.DataFrame(rows)

def load_all_financial_metrics(metadata_csv: str) -> pd.DataFrame:
    global CIK_MAPPING
    CIK_MAPPING = load_cik_mapping(metadata_csv)

    meta_df = pd.read_csv(metadata_csv)
    tickers = meta_df['ticker'].str.upper().unique().tolist()

    all_frames = []
    for ticker in tickers:
        try:
            cik = CIK_MAPPING[ticker]
            facts_json = get_facts_json(cik)
            df_facts = facts_to_df(facts_json, ticker)
            if not df_facts.empty:
                last_filing_date = df_facts['end'].max()
                recent_df = df_facts[df_facts['end'] == last_filing_date].copy()
                row = meta_df[meta_df['ticker'].str.upper() == ticker].iloc[0]
                recent_df['company_name'] = row.get('company_name', pd.NA)
                recent_df['industry_name'] = row.get('industry_name', pd.NA)
                recent_df['segment'] = row.get('segment', pd.NA)
                all_frames.append(recent_df)
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {e}")

    if not all_frames:
        return pd.DataFrame()
    return pd.concat(all_frames, ignore_index=True)

# =============================================================================
#                                 MAIN
# =============================================================================

def main():
    key_path = "/home/eraphaelparra/aialchemy.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    metadata_csv_path = "/home/eraphaelparra/profit-scout/data/sp500_metadata.csv"
