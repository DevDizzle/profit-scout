#!/usr/bin/env python
import os
import datetime
import requests
import pandas as pd
from sec_api import QueryApi, RenderApi
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials and configuration from environment variables
SEC_API_KEY = os.environ.get("SEC_API_KEY")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "aialchemy_bucket")

# Initialize Google Cloud Storage client and bucket
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)

# Define the date range for filings
start_date = "2023-01-01"
end_date = datetime.date.today().strftime("%Y-%m-%d")

def download_filing_pdf(ticker, start_date, end_date, form_type):
    """
    Download the most recent filing of the specified form type (e.g. "10-K" or "10-Q")
    as a PDF using the SEC API.
    
    Returns:
        local_path (str): Local file path of the downloaded PDF.
        file_name (str): The generated file name.
    If no filing is found, returns (None, None).
    """
    query = {
        "query": f'ticker:"{ticker}" AND formType:"{form_type}" AND filedAt:[{start_date} TO {end_date}]',
        "from": 0,
        "size": 10,
        "sort": [{"filedAt": {"order": "desc"}}],
    }
    query_api = QueryApi(SEC_API_KEY)
    # RenderApi is initialized for potential alternative formatting
    _ = RenderApi(SEC_API_KEY)
    
    response = query_api.get_filings(query)
    if response.get("filings"):
        metadata = response["filings"][0]
        filing_url = metadata["linkToFilingDetails"]
        date_str = metadata["filedAt"][:10]
        file_name = f"{ticker}_{date_str}_{metadata['formType'].replace('/A','')}_{filing_url.split('/')[-1]}.pdf"
        
        api_url = f"https://api.sec-api.io/filing-reader?token={SEC_API_KEY}&type=pdf&url={filing_url}"
        r = requests.get(api_url, stream=True)
        r.raise_for_status()
        
        # Save the PDF to a temporary local folder
        local_folder = "filings"
        if not os.path.isdir(local_folder):
            os.makedirs(local_folder)
        local_path = os.path.join(local_folder, file_name)
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return local_path, file_name
    else:
        return None, None

def get_sp500_tickers():
    """
    Retrieve the list of S&P 500 tickers from Wikipedia.
    """
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df_sp500 = pd.read_html(sp500_url, header=0)[0]
    # Adjust ticker symbols if necessary (e.g., replace dots with hyphens)
    tickers = df_sp500['Symbol'].str.replace('.', '-', regex=False).tolist()
    return tickers

def main():
    tickers = get_sp500_tickers()
    for ticker in tickers:
        print(f"Processing {ticker}...")
        # Attempt to download a 10-K filing; if not found, try 10-Q
        local_file, file_name = download_filing_pdf(ticker, start_date, end_date, form_type="10-K")
        if local_file is None:
            local_file, file_name = download_filing_pdf(ticker, start_date, end_date, form_type="10-Q")
        
        if local_file:
            blob_path = f"SEC/{file_name}"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_file)
            print(f"Uploaded {file_name} to gs://{GCS_BUCKET_NAME}/{blob_path}")
            # Optionally remove the local file after upload
            os.remove(local_file)
        else:
            print(f"No filing found for {ticker} between {start_date} and {end_date}.")

if __name__ == '__main__':
    main()
