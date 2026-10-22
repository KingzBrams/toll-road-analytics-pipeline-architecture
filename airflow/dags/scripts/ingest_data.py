import requests
import logging
logger = logging.getLogger(__name__)
import pandas as pd

def extract_data_from_api(**kwargs) -> list[dict]:
    """Extract Data API Full Load"""
    
    limit = kwargs.get("limit", 5000)
    last_watermark = kwargs.get("last_watermark")
    api_url = kwargs.get("api_url")
    
    params = {"limit": limit}
    if last_watermark:
        params["updated_since"] = last_watermark

    try:
        responses = requests.get(api_url, params=params, timeout=30)
        responses.raise_for_status()
        result_json = responses.json()
        raw_data_json = result_json.get("data", [])
        
        logger.info(f"✅ Success extract {len(raw_data_json)} rows data from API.")  
        
        kwargs["ti"].xcom_push(key='data_json', value=raw_data_json)

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed extract data from API {api_url}: {e}")
        raise

def validator_data_from_api(**kwargs) -> pd.DataFrame:
    """Validator Data From API, Check Column and Format Data"""
    pass    

def ingest_data_into_clickhouse(**kwargs) -> pd.DataFrame:
    """Ingest Data Into Clickhouse, Use Batch Insert"""
    pass

