from google.cloud import bigquery
from app.utils.logger import logger

bq_client = bigquery.Client()
BQ_STOCK_TABLE = "aialchemy.financial_data.sp500_metadata"
BQ_RATIOS_TABLE = "aialchemy.financial_data.financial_ratios"

def validate_stock(stock_query: str):
    logger.debug(f"🔍 Validating stock in BigQuery: {stock_query}")
    query = f"""
    SELECT ticker, company_name 
    FROM `{BQ_STOCK_TABLE}`
    WHERE LOWER(ticker) = @stock_query OR LOWER(company_name) = @stock_query
    LIMIT 1
    """
    params = [bigquery.ScalarQueryParameter("stock_query", "STRING", stock_query.lower())]

    try:
        results = list(bq_client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)))
        if results:
            logger.info(f"✅ Stock validated: {results[0]['company_name']} ({results[0]['ticker']})")
            return results[0]
        else:
            logger.warning(f"❌ Stock not found in BigQuery: {stock_query}")
            return None
    except Exception as e:
        logger.error(f"❌ BigQuery error: {e}")
        return None

def get_financial_ratios(ticker: str):
    logger.debug(f"📊 Querying financial ratios for ticker: {ticker}")

    query = f"""
        SELECT
            ROE,
            Debt_to_Equity,
            Current_Ratio,
            Gross_Margin,
            P_E_Ratio,
            FCF_Yield
        FROM `{BQ_RATIOS_TABLE}`
        WHERE LOWER(ticker) = @ticker
        ORDER BY as_of_date DESC
        LIMIT 1
    """

    params = [bigquery.ScalarQueryParameter("ticker", "STRING", ticker.lower())]

    try:
        query_job = bq_client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
        result = query_job.result()
        row = next(result, None)

        if not row:
            logger.warning(f"❌ No financial ratios found for {ticker}")
            return None

        ratios = {
            "ROE": row["ROE"],
            "Debt_to_Equity": row["Debt_to_Equity"],
            "Current_Ratio": row["Current_Ratio"],
            "Gross_Margin": row["Gross_Margin"],
            "P_E_Ratio": row["P_E_Ratio"],
            "FCF_Yield": row["FCF_Yield"],
        }

        logger.info(f"✅ Retrieved financial ratios for {ticker}: {ratios}")
        return ratios

    except Exception as e:
        logger.error(f"❌ Error fetching financial ratios for {ticker}: {e}")
        return None
