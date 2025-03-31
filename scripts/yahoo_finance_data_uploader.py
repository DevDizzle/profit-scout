#!/usr/bin/env python
import os
import yfinance as yf
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve configuration from environment variables
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "aialchemy_bucket")

# Initialize Google Cloud Storage client and bucket
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)

def get_sp500_tickers():
    """
    Retrieve the list of S&P 500 tickers from Wikipedia.
    """
    sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df_sp500 = pd.read_html(sp500_url, header=0)[0]
    tickers = df_sp500['Symbol'].str.replace('.', '-', regex=False).tolist()
    return tickers

def main():
    tickers = get_sp500_tickers()
    for ticker in tickers:
        try:
            print(f"Processing {ticker}...")
            stock = yf.Ticker(ticker)
            
            # Fetch financial data from Yahoo Finance
            financials = stock.financials.reset_index()
            balance_sheet = stock.balance_sheet.reset_index()
            cashflow = stock.cashflow.reset_index()
            info = stock.info
            prices = stock.history(period='2y').reset_index()  # 2 years of price history
            
            # Prepare CSV content with section markers
            data_lines = []
            data_lines.append('[info]')
            data_lines.append(f"market_cap,{info.get('marketCap', 'N/A')}")
            data_lines.append(f"shares_outstanding,{info.get('sharesOutstanding', 'N/A')}")
            
            data_lines.append('[financials]')
            financials_csv = financials.to_csv(index=False, header=True)
            data_lines.extend(financials_csv.splitlines())
            
            data_lines.append('[balance_sheet]')
            balance_sheet_csv = balance_sheet.to_csv(index=False, header=True)
            data_lines.extend(balance_sheet_csv.splitlines())
            
            data_lines.append('[cashflow]')
            cashflow_csv = cashflow.to_csv(index=False, header=True)
            data_lines.extend(cashflow_csv.splitlines())
            
            data_lines.append('[prices]')
            prices_csv = prices.to_csv(index=False, header=True)
            data_lines.extend(prices_csv.splitlines())
            
            csv_content = '\n'.join(data_lines)
            csv_filename = f"{ticker}.csv"
            with open(csv_filename, 'w') as f:
                f.write(csv_content)
            
            # Upload CSV to GCS under "Yahoo Finance/" folder
            blob_name = f"Yahoo Finance/{csv_filename}"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(csv_filename)
            print(f"Uploaded {csv_filename} to gs://{GCS_BUCKET_NAME}/{blob_name}")
            
            # Optionally remove the local CSV file after upload
            os.remove(csv_filename)
        
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

if __name__ == '__main__':
    main()
