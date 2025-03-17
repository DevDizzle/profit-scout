#!/usr/bin/env python3
import requests
import pandas as pd
import logging
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
    "AES": "0000874761",
    "NTAP": "0001002047",
    "GIS": "0000040704",
    "NOC": "0001133421",
    "CMI": "0000026172"
}

def _facts_url(cik: str) -> str:
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3))
def get_facts_json(ticker: str) -> dict:
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
    Convert 'us-gaap' portion of XBRL facts into a DataFrame.
    Extracts the fact key, label, unit, value and other metadata.
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

def export_raw_data(tickers, output_file="raw_fact_keys.csv"):
    all_data = []
    for ticker in tickers:
        try:
            logger.info(f"Processing ticker {ticker}...")
            facts_json = get_facts_json(ticker)
            df = facts_to_df(facts_json)
            df['ticker'] = ticker
            logger.info(f"Retrieved {len(df)} fact rows for {ticker}")
            all_data.append(df)
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
