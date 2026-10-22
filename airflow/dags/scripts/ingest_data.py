import requests
import logging
logger = logging.getLogger("airflow.task")
import pandas as pd
from pathlib import Path
import json

# ---------------------------------------------------------
# TASK: EXTRACT DATA FROM API 
# ---------------------------------------------------------
def extract_data_from_api(**kwargs) -> str:
  """Extract Data API dengan pagination dan metode Append ke File (Format JSONL)."""
  
  limit = kwargs.get("limit", 5000)
  last_watermark = kwargs.get("last_watermark")
  api_url = kwargs.get("api_url")
  file_name = kwargs.get("file_name", "raw_data")

  params = {"limit": limit}
  if last_watermark:
    filter_field = kwargs.get("filter_key", "updated_at")
    params[filter_field] = last_watermark

  raw_dir = Path("/opt/airflow/logs/raw_data")
  raw_dir.mkdir(parents=True, exist_ok=True)

  # Menggunakan ekstensi .jsonl (JSON Lines) agar aman di-append per baris
  file_path = raw_dir / f"{file_name}.jsonl"

  # Hapus file lama jika ada sebelum proses loop (agar data run sebelumnya tidak ikut ter-append)
  if file_path.exists():
    file_path.unlink()

  total_fetched = 0
  offset = 0
  page = 1

  logger.info("=" * 150)
  logger.info(f"🔹 START APPEND EXTRACTION: [{api_url}]")

  try:
    # Membuka file dengan mode 'a' (append) selama proses pagination berlangsung
    with open(file_path, "a", encoding="utf-8") as f:
      while True:
        params["offset"] = offset
        logger.info(f"-> Fetching page {page} with params: {params}")

        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()

        result_json = response.json()
        page_data = result_json.get("data", [])

        if not page_data:
          # Jika halaman kosong, hentikan loop
          break

        # Tulis setiap baris data langsung ke file secara append (baris per baris)
        for item in page_data:
          f.write(json.dumps(item, ensure_ascii=False) + "\n")

        total_fetched += len(page_data)

        # Jika data yang ditarik kurang dari limit, berarti ini halaman terakhir
        if len(page_data) < limit:
          break

        offset += limit
        page += 1

    logger.info(
        f"✅ SUCCESS: Appended total {total_fetched} rows across {page}"
        f" page(s) to {file_path}"
    )
    logger.info("=" * 150)

    # Push filepath string ke XCom
    kwargs["ti"].xcom_push(key="data_json", value=str(file_path))
    return str(file_path)

  except requests.exceptions.RequestException as e:
    logger.error(f"❌ ERROR: Failed during append extraction. Details: {e}")
    logger.info("=" * 50)
    raise

# ---------------------------------------------------------
# TASK: VALIDATOR DATA FROM API 
# -------------------------------------------------------   --
def universal_base_validator(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Universal Validator Data From API"""

    clean_data = pd.DataFrame(raw_data)
    
    # handle missing values and trim whitespace
    str_cols = clean_data.select_dtypes(include=['object']).columns
    for col in str_cols:
        clean_data[col] = clean_data[col].astype(str).str.strip()
        clean_data[col] = clean_data[col].replace({
            "nan": None,
            "NaN": None,
            "NULL": None,
            "NaT": None,
            "": None,
            "-": None,
            "N/A": None,
            "n/a": None,
            "Null": None
        }) 

    # drop duplicate data
    clean_data = clean_data.drop_duplicates()

    logger.info(f"[+] Universal validation passed. Rows: {len(clean_data)}")

    return clean_data

def validate_employees_data(**kwargs):
    """Validate Employees Data"""
    file_path_raw_employee = kwargs['ti'].xcom_pull(task_ids='ingest_data_master_employee.extract_employee_task', key='data_json')

    logger.info("=" * 150)    
    raw_data = pd.read_json(file_path_raw_employee, lines=True)
    clean_data = universal_base_validator(raw_data)
    logger.info("=" * 150)  
    
def validate_providers_data(**kwargs):
    """Providers Validator"""
    file_path_raw_provider = kwargs['ti'].xcom_pull(task_ids='ingest_data_master_provider.extract_provider_task', key='data_json')

    logger.info("=" * 150)    
    raw_data = pd.read_json(file_path_raw_provider, lines=True)
    clean_data = universal_base_validator(raw_data)
    logger.info("=" * 150)  

def validate_competencies_data(**kwargs):
    """Competencies Validator"""
    file_path_raw_competency = kwargs['ti'].xcom_pull(task_ids='ingest_data_master_competency.extract_competency_task', key='data_json')

    logger.info("=" * 150)    
    raw_data = pd.read_json(file_path_raw_competency, lines=True)
    clean_data = universal_base_validator(raw_data)
    logger.info("=" * 150)  

def validate_units_data(**kwargs):
    """Units Validator"""
    file_path_raw_unit = kwargs['ti'].xcom_pull(task_ids='ingest_data_master_unit.extract_unit_task', key='data_json')

    logger.info("=" * 150)    
    raw_data = pd.read_json(file_path_raw_unit, lines=True)
    clean_data = universal_base_validator(raw_data)
    logger.info("=" * 150)  

def validate_activity_data(**kwargs):
    """Activity Validator"""
    file_path_raw_activity = kwargs['ti'].xcom_pull(task_ids='ingest_data_fact_activities.extract_activities_task', key='data_json')

    logger.info("=" * 150)    
    raw_data = pd.read_json(file_path_raw_activity, lines=True)
    clean_data = universal_base_validator(raw_data)
    logger.info("=" * 150)  

# ---------------------------------------------------------
# TASK: INGEST DATA INTO CLICKHOUSE 
# ---------------------------------------------------------
def ingest_data_into_clickhouse(**kwargs) -> pd.DataFrame:
    """Ingest Data Into Clickhouse, Use Batch Insert"""
    pass

