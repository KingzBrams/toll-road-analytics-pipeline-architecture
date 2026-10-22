import requests
import logging
logger = logging.getLogger("airflow.task")
import pandas as pd

def extract_data_from_api(**kwargs) -> list[dict]:
    """Extract Data API Full Load"""
    
    limit = kwargs.get("limit", 5000)
    last_watermark = kwargs.get("last_watermark") 
    api_url = kwargs.get("api_url")
    
    params = {"limit": limit}

    if last_watermark:
        filter_field = kwargs.get("filter_key", "updated_at")
        params[filter_field] = last_watermark

    logger.info("=" * 150)
    logger.info(f"🔹 START EXTRACTION: [{api_url}]")
    logger.info(f"🔹 PARAMETERS SENT: [{params}]")

    try:
        responses = requests.get(api_url, params=params, timeout=30)
        responses.raise_for_status()
        result_json = responses.json()
        raw_data_json = result_json.get("data", [])
        
        logger.info(f"✅ SUCCESS: Fetched {len(result_json)} rows from API.")        
        logger.info("=" * 150)
        kwargs["ti"].xcom_push(key='data_json', value=raw_data_json)

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ ERROR: Failed to extract data. Details: {e}")
        logger.info("=" * 50)
        raise

def validator_data_from_api(**kwargs) -> pd.DataFrame:
    """Validator Data From API, Check Column and Format Data"""
    pass    

def ingest_data_into_clickhouse(**kwargs) -> pd.DataFrame:
    """Ingest Data Into Clickhouse, Use Batch Insert"""
    pass

