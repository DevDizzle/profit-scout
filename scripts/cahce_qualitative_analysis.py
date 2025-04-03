# batch_qualitative_analysis.py

import os
import pandas as pd
from google.cloud import storage
import google.generativeai as genai
import re
import tempfile
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging

# --- Configuration via Environment Variables ---

# Load sensitive info and config from environment variables
# Required:
GOOGLE_CREDS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') # Standard variable for GCS auth
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# Optional with Defaults:
GCS_SOURCE_PDF_PREFIX = os.getenv('GCS_SOURCE_PDF_PREFIX', 'SEC/')
GCS_QUALITATIVE_TXT_PREFIX = os.getenv('GCS_QUALITATIVE_TXT_PREFIX', 'Qualitative_Analysis_TXT/')
GCS_METADATA_CSV_PREFIX = os.getenv('GCS_METADATA_CSV_PREFIX', 'Qualitative_Metadata/')
GEMINI_MODEL_NAME = os.getenv('GEMINI_MODEL_NAME', "gemini-1.5-flash")
GEMINI_TEMPERATURE = float(os.getenv('GEMINI_TEMPERATURE', 0.1))
GEMINI_MAX_TOKENS = int(os.getenv('GEMINI_MAX_TOKENS', 4096))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 5)) # Default to 5 workers

# --- End Configuration ---

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- Validation for Required Configuration ---
if not GOOGLE_CREDS_PATH:
    logging.error("❌ Environment variable GOOGLE_APPLICATION_CREDENTIALS not set.")
    raise SystemExit("Missing GOOGLE_APPLICATION_CREDENTIALS")
if not os.path.exists(GOOGLE_CREDS_PATH):
     logging.error(f"❌ Credentials file not found at path: {GOOGLE_CREDS_PATH}")
     raise SystemExit(f"Credentials file not found at specified path.")
if not GCS_BUCKET_NAME:
    logging.error("❌ Environment variable GCS_BUCKET_NAME not set.")
    raise SystemExit("Missing GCS_BUCKET_NAME")
if not GEMINI_API_KEY:
    logging.error("❌ Environment variable GEMINI_API_KEY not set.")
    raise SystemExit("Missing GEMINI_API_KEY")


# --- Initialize Clients (Done once globally) ---
# Note: GCS client uses GOOGLE_APPLICATION_CREDENTIALS automatically if set
try:
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    logging.info("✅ Google Cloud Storage client initialized.")
except Exception as e:
    logging.error(f"❌ Error initializing GCS Client: {e}")
    raise SystemExit("GCS Client Initialization Failed")

try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        GEMINI_MODEL_NAME,
        generation_config={"temperature": GEMINI_TEMPERATURE, "max_output_tokens": GEMINI_MAX_TOKENS}
    )
    logging.info("✅ Google Gemini client initialized.")
except Exception as e:
    logging.error(f"❌ Error initializing Gemini Client: {e}")
    raise SystemExit("Gemini Client Initialization Failed")

# --- Define Analysis Prompt ---
# (Using the improved prompt from previous steps)
prompt = """
You are an expert financial analyst reviewing an SEC filing (10-K or 10-Q). Your task is to extract key financial performance indicators AND synthesize them into a qualitative assessment based *strictly* on the information presented within this document.

Follow these steps:

1.  **Extract Key Financial Data & Trends**:
    * Identify and list primary financial figures and trends...

2.  **Identify Key Qualitative Signals from the Filing**:
    * Based *only* on the extracted data... identify the **Top 2-3 Qualitative Strengths**...
    * Similarly, identify the **Top 2-3 Qualitative Weaknesses or Concerns**...

3.  **Synthesize Overall Qualitative Assessment**:
    * Provide a brief summary paragraph...
    * Conclude this paragraph by stating whether the overall qualitative picture... predominantly **Positive**, **Negative**, or **Mixed**.

4.  **Summarize Investment Implications (from Filing)**:
    * Briefly list the key factors...
    * **Do NOT provide a final Buy/Sell/Hold recommendation.**...

**Output Format**:
Your output should be clearly structured... Ensure the analysis remains objective and grounded *exclusively* in the provided SEC filing text...

CRITICAL INSTRUCTION: Your response MUST contain ONLY the requested analysis sections (Key Data/Trends, Qualitative Signals, Qualitative Assessment, Investment Implications). Do NOT include any introductory sentences, concluding remarks, warnings, or disclaimers stating that this is not financial advice or that the analysis is based only on the provided text. Output *only* the structured analysis itself.
"""

# --- Helper Functions ---
# (extract_info_from_filename and get_company_name are unchanged)
def extract_info_from_filename(gcs_path):
    filename = gcs_path.split('/')[-1]
    pattern = r'^([A-Z.]{1,5})_(\d{4}-\d{2}-\d{2})_([A-Za-z0-9-]+)_([a-z.]{1,5})-(\d{8})\.htm\.pdf$'
    match = re.match(pattern, filename)
    if match:
        ticker_upper, filing_date, form_type, ticker_lower, period_end_date = match.groups()
        return filing_date, ticker_upper.replace('.', '-'), form_type.upper(), period_end_date
    else:
        return None, None, None, None

def get_company_name(ticker, sp500_map=None):
    # TODO: Implement actual fetching using an S&P500 list/map if needed
    return f"{ticker} Inc." # Fallback placeholder


# --- Function to Process a Single Blob (for Threading) ---

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception) # Consider refining retry exceptions
)
def upload_to_gemini_with_retry(local_path, ticker):
    """Wrapper for genai.upload_file with tenacity retry logic."""
    logging.info(f"[{ticker}] Attempting upload to Gemini Files API...")
    uploaded_file = genai.upload_file(path=local_path)
    return uploaded_file

def process_blob(blob_name, temp_dir_base):
    """
    Processes a single 10-K PDF blob: download, analyze, upload results.
    Returns a status string: 'success', 'error_upload', 'error_generate', 'error_unexpected', 'error_skipped_in_thread'.
    """
    local_pdf_path = None
    local_txt_path = None
    local_csv_path = None
    uploaded_file_gemini = None
    status = 'error_unexpected'
    ticker = "UNKNOWN" # Initialize ticker for logging in case extraction fails

    # Re-create blob object within the thread
    # Note: storage_client and bucket are global and assumed thread-safe for read operations like blob()
    blob = bucket.blob(blob_name)

    try:
        filing_date, ticker, form_type, period_end = extract_info_from_filename(blob.name)
        if not ticker or form_type != '10-K':
             # This path should ideally not be reached due to pre-filtering
             logging.warning(f"[{ticker or 'PARSE_FAILED'}] Skipped in thread (should have been pre-filtered).")
             return 'error_skipped_in_thread'

        logging.info(f"[{ticker}] Thread processing 10-K: {blob.name}")

        # --- Use NamedTemporaryFile for automatic cleanup ---
        with tempfile.NamedTemporaryFile(dir=temp_dir_base, suffix=".pdf") as temp_pdf:
            local_pdf_path = temp_pdf.name # Get path for upload/logging
            blob.download_to_filename(local_pdf_path)

            # Upload PDF to Gemini (with retry)
            try:
                uploaded_file_gemini = upload_to_gemini_with_retry(local_pdf_path, ticker)
                logging.info(f"[{ticker}] Upload successful (Gemini file: {uploaded_file_gemini.name})")
                time.sleep(2) # Consider making delay configurable or removing
            except Exception as gemini_upload_error:
                logging.error(f"[{ticker}] Error uploading to Gemini Files API after retries: {gemini_upload_error}")
                status = 'error_upload'
                raise

            # Generate analysis via Gemini
            logging.info(f"[{ticker}] Generating analysis...")
            try:
                # Ensure model is accessible (it's global)
                response = model.generate_content([prompt, uploaded_file_gemini])
                analysis_text = response.text
                logging.info(f"[{ticker}] Analysis generated (Length: {len(analysis_text)} chars).")
            except Exception as gemini_generate_error:
                logging.error(f"[{ticker}] Error generating content: {gemini_generate_error}")
                status = 'error_generate'
                raise
        # --- PDF temp file automatically deleted here ---
        local_pdf_path = None # Reset path after file is closed/deleted

        # Define fixed output filenames
        txt_filename = f"{ticker}_latest_10K_analysis.txt"
        csv_filename = f"{ticker}_latest_10K_metadata.csv"

        # Save analysis text to local temp file (use NamedTemporaryFile again)
        with tempfile.NamedTemporaryFile(mode='w', dir=temp_dir_base, suffix=".txt", encoding='utf-8', delete=False) as temp_txt:
            local_txt_path = temp_txt.name
            temp_txt.write(analysis_text)

        # Upload analysis text file to GCS
        gcs_txt_path = f"{GCS_QUALITATIVE_TXT_PREFIX}{txt_filename}"
        txt_blob_to_upload = bucket.blob(gcs_txt_path) # Use global bucket reference
        txt_blob_to_upload.upload_from_filename(local_txt_path)
        analysis_txt_url = txt_blob_to_upload.public_url # Assumes public access
        logging.info(f"[{ticker}] Analysis TXT uploaded to: gs://{GCS_BUCKET_NAME}/{gcs_txt_path}")

        # Get company name
        company_name = get_company_name(ticker)

        # Create Metadata DataFrame
        df = pd.DataFrame({
            'Reporting_Period': [period_end], 'Filing_Date': [filing_date],
            'Stock_Ticker': [ticker], 'Company_Name': [company_name],
            'Form_Type': [form_type], 'Source_PDF_Path': [f"gs://{GCS_BUCKET_NAME}/{blob.name}"],
            'Analysis_Link': [analysis_txt_url]
        })

        # Save metadata DataFrame to local CSV (use NamedTemporaryFile)
        with tempfile.NamedTemporaryFile(mode='w', dir=temp_dir_base, suffix=".csv", encoding='utf-8', delete=False) as temp_csv:
            local_csv_path = temp_csv.name
            df.to_csv(local_csv_path, index=False)

        # Upload metadata CSV to GCS
        gcs_csv_path = f"{GCS_METADATA_CSV_PREFIX}{csv_filename}"
        csv_blob = bucket.blob(gcs_csv_path)
        csv_blob.upload_from_filename(local_csv_path)
        logging.info(f"[{ticker}] Metadata CSV uploaded to: gs://{GCS_BUCKET_NAME}/{gcs_csv_path}")

        logging.info(f"[{ticker}] ✅ Successfully processed 10-K.")
        status = 'success'

    except Exception as e:
        # Status will be 'error_upload', 'error_generate', or 'error_unexpected'
        logging.error(f"[{ticker}] Worker Error processing {blob_name}: {e}", exc_info=True) # Log traceback for worker errors

    finally:
        # Clean up manually created local temp files (if not using delete=True with NamedTemporaryFile)
        # Using delete=False above to ensure file exists for upload, so manual deletion is needed.
        if local_txt_path and os.path.exists(local_txt_path): os.remove(local_txt_path)
        if local_csv_path and os.path.exists(local_csv_path): os.remove(local_csv_path)
        # PDF path should be None here as it was deleted by its 'with' block if NamedTemporaryFile used correctly

        # Clean up Gemini uploaded file
        if uploaded_file_gemini:
            try:
                logging.info(f"[{ticker}] Cleaning up Gemini file: {uploaded_file_gemini.name}")
                genai.delete_file(uploaded_file_gemini.name)
            except Exception as delete_err:
                logging.warning(f"[{ticker}] Failed to delete Gemini file {uploaded_file_gemini.name}: {delete_err}")

    return status

# --- Main Execution Logic ---
def main():
    """Main function to orchestrate the batch processing."""
    logging.info("Starting PDF processing with ThreadPoolExecutor...")
    # (Initialize summary counters - remove 'skipped_existing')
    results_summary = {'success': 0, 'error_upload': 0, 'error_generate': 0, 'error_unexpected': 0, 'error_skipped_in_thread': 0, 'skipped_non_10k': 0, 'skipped_parse': 0}
    futures = []
    processed_blob_names = set()

    # Use TemporaryDirectory for managing local files for all threads
    with tempfile.TemporaryDirectory() as temp_dir:
        logging.info(f"Using main temporary directory: {temp_dir}")

        blobs = list(bucket.list_blobs(prefix=GCS_SOURCE_PDF_PREFIX))
        logging.info(f"Found {len(blobs)} total blobs under prefix '{GCS_SOURCE_PDF_PREFIX}'. Filtering and submitting tasks...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for blob in blobs:
                if blob.name in processed_blob_names: continue
                if not blob.name.lower().endswith('.pdf'): continue

                filing_date, ticker, form_type, period_end = extract_info_from_filename(blob.name)

                if not ticker:
                    results_summary['skipped_parse'] += 1
                    continue

                if not form_type or form_type != '10-K':
                    results_summary['skipped_non_10k'] += 1
                    continue

                # --- Check for existing output file REMOVED ---

                # Submit blob for processing
                processed_blob_names.add(blob.name)
                future = executor.submit(process_blob, blob.name, temp_dir)
                futures.append(future)

            logging.info(f"Submitted {len(futures)} unique 10-K files to {MAX_WORKERS} workers...")

            processed_futures = 0
            for future in concurrent.futures.as_completed(futures):
                processed_futures += 1
                try:
                    result_status = future.result()
                    if result_status in results_summary:
                        results_summary[result_status] += 1
                    else:
                        logging.warning(f"Received unexpected status '{result_status}' from worker.")
                        results_summary['error_unexpected'] += 1
                    # Log progress periodically
                    if processed_futures % 10 == 0 or processed_futures == len(futures):
                        logging.info(f"  Processed {processed_futures}/{len(futures)} submitted tasks...")
                except Exception as e:
                    logging.error(f"ERROR retrieving result from future: {e}", exc_info=True)
                    results_summary['error_unexpected'] += 1


    logging.info("\n--- Processing Summary ---")
    total_skipped = results_summary['skipped_non_10k'] + results_summary['skipped_parse']
    logging.info(f"Total Blobs Found: {len(blobs)}")
    logging.info(f"Skipped (Non-10K / Unparseable): {total_skipped}")
    logging.info(f"Successfully Processed 10-K Filings: {results_summary['success']}")
    total_errors = results_summary['error_upload'] + results_summary['error_generate'] + results_summary['error_unexpected'] + results_summary['error_skipped_in_thread']
    logging.info(f"Errors Encountered during Processing: {total_errors}")
    logging.info(f"  (Upload: {results_summary['error_upload']}, Generate: {results_summary['error_generate']}, Unexpected: {results_summary['error_unexpected']}, SkippedInThread: {results_summary['error_skipped_in_thread']})")
    logging.info("Processing complete!")

# --- Script Execution ---
if __name__ == "__main__":
    # Print configuration being used (excluding sensitive keys)
    print("--- Running Configuration ---")
    print(f"GCS_BUCKET_NAME={GCS_BUCKET_NAME}")
    print(f"GCS_SOURCE_PDF_PREFIX={GCS_SOURCE_PDF_PREFIX}")
    print(f"GCS_QUALITATIVE_TXT_PREFIX={GCS_QUALITATIVE_TXT_PREFIX}")
    print(f"GCS_METADATA_CSV_PREFIX={GCS_METADATA_CSV_PREFIX}")
    print(f"GEMINI_MODEL_NAME={GEMINI_MODEL_NAME}")
    print(f"MAX_WORKERS={MAX_WORKERS}")
    print("---")

    main() # Run the main processing logic
