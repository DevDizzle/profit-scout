import requests
import pandas as pd
from google.cloud import bigquery
from bs4 import BeautifulSoup

# Google Cloud Config
PROJECT_ID = "aialchemy"
DATASET_ID = "financial_data"
TABLE_ID = "sp500_metadata"

# SEC API Endpoints
SEC_CIK_LOOKUP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_METADATA_URL = "https://data.sec.gov/submissions/CIK{}.json"

# Headers for SEC API requests
HEADERS = {"User-Agent": "aialchemy (eraphaelparra@gmail.com)"}

def get_sp500_companies():
    """Fetch S&P 500 company list from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"class": "wikitable"})
    rows = table.find_all("tr")[1:]  # Skip header row

    sp500_companies = []
    for row in rows:
        columns = row.find_all("td")
        ticker = columns[0].text.strip()
        company_name = columns[1].text.strip()
        sp500_companies.append((ticker, company_name))

    return sp500_companies

def get_cik_for_ticker(ticker):
    """Fetch CIK for a given stock ticker from SEC API."""
    try:
        response = requests.get(SEC_CIK_LOOKUP_URL, headers=HEADERS)
        data = response.json()

        for company in data.values():
            if company["ticker"].upper() == ticker.upper():
                return str(company["cik_str"]).zfill(10)  # Pad CIK to 10 digits
    except Exception as e:
        print(f"Error fetching CIK for {ticker}: {e}")

    return None

def get_sic_info(cik):
    """Fetch SIC code and industry name from SEC EDGAR API."""
    try:
        response = requests.get(SEC_METADATA_URL.format(cik), headers=HEADERS)
        data = response.json()
        return data.get("sic", None), data.get("sicDescription", None)
    except Exception as e:
        print(f"Error fetching SIC for CIK {cik}: {e}")
        return None, None

def load_sp500_to_bigquery():
    """Fetch S&P 500 data, retrieve CIK and SIC codes, and store in BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    sp500_companies = get_sp500_companies()
    rows_to_insert = []

    for ticker, company_name in sp500_companies:
        cik = get_cik_for_ticker(ticker)
        if cik:
            sic_code, industry_name = get_sic_info(cik)
            sec_filing_url = f"https://www.sec.gov/edgar/browse/?CIK={cik}"
            rows_to_insert.append((ticker, company_name, cik, sic_code, industry_name, sec_filing_url))

    # Convert to DataFrame
    df = pd.DataFrame(rows_to_insert, columns=["ticker", "company_name", "cik", "sic_code", "industry_name", "sec_filing_url"])

    # BigQuery Write Config
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Write Data to BigQuery
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    print(f"Successfully loaded {len(df)} S&P 500 companies into {TABLE_ID}")

if __name__ == "__main__":
    load_sp500_to_bigquery()
