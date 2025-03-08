import os
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import logging
import re

# Define headers for SEC requests
headers = {
    "User-Agent": "ProfitScout (eraphaelparra@gmail.com)",  # Replace with your contact info
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov"
}

# Global CIK mapping variable that will be populated from the metadata CSV
CIK_MAPPING = {}

def load_cik_mapping(metadata_csv):
    """
    Load the SP500 metadata CSV and build a dictionary mapping tickers to CIKs.
    Assumes the CSV has columns 'ticker' and 'cik'.
    """
    df = pd.read_csv(metadata_csv)
    cik_map = {row['ticker'].upper(): str(row['cik']).zfill(10) for index, row in df.iterrows()}
    print(f"[INFO] Loaded CIK mapping for {len(cik_map)} tickers from {metadata_csv}.")
    return cik_map

def cik_matching_ticker(ticker):
    """Return the CIK for a given ticker using the global mapping."""
    ticker = ticker.upper()
    if ticker in CIK_MAPPING:
        print(f"[INFO] Found CIK for {ticker}: {CIK_MAPPING[ticker]}")
        return CIK_MAPPING[ticker]
    else:
        raise ValueError(f"CIK not found for ticker {ticker}")

def get_submission_data_for_ticker(ticker, only_filings_df=False):
    """Retrieve submission data from SEC for a given ticker."""
    cik = cik_matching_ticker(ticker)
    url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    print(f"[INFO] Retrieved submission data for {ticker}.")
    if only_filings_df:
        filings = data.get("filings", {}).get("recent", {})
        df = pd.DataFrame(filings)
        return df
    return data

def get_filtered_filings(ticker, form_type, only_accession_numbers=False):
    """Filter filings for a given form type (e.g., '10-Q')."""
    df = get_submission_data_for_ticker(ticker, only_filings_df=True)
    filtered_df = df[df['form'].str.contains(form_type, case=False, na=False)]
    print(f"[INFO] Filtered filings for {ticker} with form type '{form_type}'; found {len(filtered_df)} filings.")
    if only_accession_numbers:
        return filtered_df['accessionNumber'].tolist()
    return filtered_df

def get_facts(ticker):
    """Retrieve XBRL facts for a given ticker."""
    cik = cik_matching_ticker(ticker)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print(f"[INFO] Retrieved XBRL facts for {ticker}.")
    return response.json()

def facts_to_df(company_facts):
    """Convert the SEC company facts into a DataFrame."""
    df = pd.DataFrame()
    for report in company_facts.get('facts', {}).get('us-gaap', {}).values():
        for unit in report.get('units', {}).values():
            temp_df = pd.DataFrame(unit)
            temp_df['item'] = report.get('label')
            df = pd.concat([df, temp_df], ignore_index=True)
    print(f"[INFO] Converted facts to DataFrame with shape {df.shape}.")
    return df

def get_recent_10q_metrics(ticker):
    """Retrieve the most recent 10-Q filing's financial metrics for a given ticker."""
    print(f"[INFO] Processing 10-Q metrics for {ticker}...")
    # Get the most recent 10-Q accession number
    accession_numbers = get_filtered_filings(ticker, "10-Q", only_accession_numbers=True)
    if not accession_numbers:
        raise ValueError(f"No 10-Q filings found for ticker {ticker}")
    recent_accession_number = accession_numbers[0]
    print(f"[INFO] Using accession number {recent_accession_number} for ticker {ticker}.")

    # Get facts data and convert to DataFrame
    facts = get_facts(ticker)
    df_facts = facts_to_df(facts)

    # Convert the 'end' field to datetime and filter for the most recent filing period
    df_facts['date'] = pd.to_datetime(df_facts['end'])
    recent_date = df_facts['date'].max()
    recent_10q_facts = df_facts[df_facts['date'] == recent_date]
    print(f"[INFO] Retrieved {len(recent_10q_facts)} records for {ticker} from the most recent filing date: {recent_date.date()}.")
    return recent_10q_facts

def load_all_financial_metrics(metadata_csv, output_csv):
    """
    Loop through the SP500 metadata CSV, process each ticker to extract
    financial metrics, append metadata columns, and write to output CSV.
    """
    global CIK_MAPPING
    # Load metadata and update the global CIK mapping
    metadata_df = pd.read_csv(metadata_csv)
    print(f"[INFO] Loaded metadata for {len(metadata_df)} tickers from {metadata_csv}.")
    CIK_MAPPING = {row['ticker'].upper(): str(row['cik']).zfill(10) for index, row in metadata_df.iterrows()}
    print(f"[INFO] CIK mapping updated with tickers: {list(CIK_MAPPING.keys())}")

    all_metrics = []

    for idx, row in metadata_df.iterrows():
        ticker = row['ticker']
        print(f"\n[INFO] Processing ticker: {ticker}")
        try:
            metrics_df = get_recent_10q_metrics(ticker)
            # Append SP500 metadata columns to the metrics data
            metrics_df['ticker'] = ticker
            metrics_df['company_name'] = row.get('company_name', np.nan)
            metrics_df['sic_code'] = row.get('sic_code', np.nan)
            metrics_df['industry_name'] = row.get('industry_name', np.nan)
            metrics_df['sec_filing_url'] = row.get('sec_filing_url', np.nan)

            all_metrics.append(metrics_df)
            print(f"[SUCCESS] Processed {ticker} with {len(metrics_df)} records.")
        except Exception as e:
            print(f"[ERROR] Error processing {ticker}: {e}")

    if all_metrics:
        final_df = pd.concat(all_metrics, ignore_index=True)
        final_df.to_csv(output_csv, index=False)
        print(f"\n[INFO] Saved all metrics to {output_csv} with {final_df.shape[0]} total records.")
    else:
        print("[WARN] No metrics were collected.")

if __name__ == "__main__":
    # Adjust the paths according to your project structure.
    metadata_csv_path = os.path.join("data", "sp500_metadata.csv")
    output_csv_path = os.path.join("data", "financial_metrics.csv")
    print("[INFO] Starting to load all financial metrics...")
    load_all_financial_metrics(metadata_csv_path, output_csv_path)
    print("[INFO] Completed loading financial metrics.")
