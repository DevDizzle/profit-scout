from google.cloud import bigquery
from app.utils.logger import logger  # Import custom logger

bq_client = bigquery.Client()
BQ_STOCK_TABLE = "aialchemy.financial_data.sp500_metadata"
BQ_RATIOS_TABLE = "aialchemy.financial_data.financial_ratios"

def validate_stock(stock_query: str):
    """Check if a stock exists in the S&P 500."""
    logger.debug(f"🔍 Querying BigQuery for stock validation: {stock_query}")
    query = f"""
    SELECT ticker, company_name FROM `{BQ_STOCK_TABLE}`
    WHERE LOWER(ticker) = @stock_query OR LOWER(company_name) = @stock_query
    LIMIT 1
    """
    
    params = [bigquery.ScalarQueryParameter("stock_query", "STRING", stock_query.lower())]
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job)
    except Exception as e:
        logger.error(f"❌ BigQuery error: {e}")
        return None

    if results:
        logger.info(f"✅ Stock validated: {results[0]['company_name']} ({results[0]['ticker']})")
        return results[0]
    
    logger.warning(f"❌ Stock not found in BigQuery: {stock_query}")
    return None
