import os
from fastapi import APIRouter, HTTPException
from app.services.gemini_service import synthesize_analysis
from app.utils.logger import logger
from pydantic import BaseModel
from google.cloud import storage # Added GCS import
from google.cloud.exceptions import NotFound # Added for specific exception handling

router = APIRouter(prefix="/synthesizer")

# --- Configuration ---
# Initialize Google Cloud Storage client
try:
    storage_client = storage.Client()
    # Get bucket name from env var or use default
    GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "aialchemy_bucket")
    GCS_QUALITATIVE_TXT_PREFIX = 'Qualitative_Analysis_TXT/' # Location of pre-computed txt files
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    logger.info(f"✅ Synthesizer: GCS client initialized for bucket '{GCS_BUCKET_NAME}'.")
except Exception as e:
    logger.error(f"❌ Synthesizer: Failed to initialize GCS Client: {e}")
    # Depending on deployment, might want to raise error or handle differently
    storage_client = None
    bucket = None
    logger.warning("Synthesizer proceeding without GCS client - Qualitative fetch will fail.")

# --- Pydantic Model Update ---
# Updated request model: No longer expects sec_analysis (qualitative)
class SynthesisRequest(BaseModel):
    ticker: str
    yahoo_analysis: dict  # Quantitative output as a JSON/dict

# --- Helper Function for GCS Fetch ---
def fetch_qualitative_analysis_text(ticker: str) -> str | None:
    """
    Fetches the pre-computed qualitative analysis text file from GCS.
    Returns the text content as a string or None if not found/error.
    """
    if not bucket:
        logger.error(f"GCS bucket not available. Cannot fetch qualitative analysis for {ticker}.")
        return None

    # Construct the expected blob name based on the convention
    blob_name = f"{GCS_QUALITATIVE_TXT_PREFIX}{ticker}_latest_10K_analysis.txt"
    logger.info(f"Attempting to fetch qualitative analysis from: gs://{GCS_BUCKET_NAME}/{blob_name}")

    blob = bucket.blob(blob_name)

    try:
        # Download the content as text
        qualitative_text = blob.download_as_text()
        logger.info(f"✅ Successfully fetched qualitative analysis text for {ticker} (Length: {len(qualitative_text)}).")
        return qualitative_text
    except NotFound:
        logger.warning(f"⚠️ Pre-computed qualitative analysis file not found at gs://{GCS_BUCKET_NAME}/{blob_name}")
        return None
    except Exception as e:
        logger.error(f"❌ Error downloading qualitative analysis text for {ticker} from GCS: {e}", exc_info=True)
        return None

# --- API Endpoint ---
@router.post("/synthesize")
async def synthesize_api(request: SynthesisRequest):
    """
    Synthesizes quantitative analysis with pre-computed qualitative analysis fetched from GCS.
    """
    logger.info(f"📡 Received request to synthesize analysis for stock: {request.ticker}")

    # 1. Fetch the pre-computed qualitative analysis text from GCS
    qualitative_text = fetch_qualitative_analysis_text(request.ticker)

    if qualitative_text is None:
        # If the file wasn't found or couldn't be read, return 404
        raise HTTPException(
            status_code=404,
            detail=f"Pre-computed qualitative analysis (latest 10-K) not found or failed to load for ticker {request.ticker}"
        )

    # 2. Call the Gemini service to synthesize
    try:
        logger.info(f"Calling Gemini service to synthesize quantitative and qualitative data for {request.ticker}...")
        synthesis = synthesize_analysis(
            ticker=request.ticker,
            yahoo_analysis=request.yahoo_analysis, # Quantitative data from request
            sec_analysis=qualitative_text          # Qualitative data fetched from GCS
        )
        logger.info(f"✅ Synthesis completed for {request.ticker}")
        return {"ticker": request.ticker, "synthesis": synthesis}
    except Exception as e:
        logger.error(f"❌ Error during synthesis call for {request.ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error during synthesis execution: {e}")
