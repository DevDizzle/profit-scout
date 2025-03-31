from fastapi import APIRouter, HTTPException
from app.services.gemini_service import synthesize_analysis
from app.utils.logger import logger
from pydantic import BaseModel

router = APIRouter(prefix="/synthesizer")

class SynthesisRequest(BaseModel):
    ticker: str
    yahoo_analysis: dict  # Quantitative output as a JSON/dict
    sec_analysis: str     # Qualitative output as a string

@router.post("/synthesize")
async def synthesize_api(request: SynthesisRequest):
    logger.info(f"📡 Received request to synthesize analysis for stock: {request.ticker}")
    try:
        synthesis = synthesize_analysis(request.ticker, request.yahoo_analysis, request.sec_analysis)
        logger.info(f"✅ Synthesis completed for {request.ticker}")
        return {"ticker": request.ticker, "synthesis": synthesis}
    except Exception as e:
        logger.error(f"❌ Error during synthesis for {request.ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Error during synthesis: {e}")
