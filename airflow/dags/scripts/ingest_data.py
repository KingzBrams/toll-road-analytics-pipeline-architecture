import requests
import logging
logger = logging.getLogger("airflow.task")
import pandas as pd
from pathlib import Path
import json
from datetime import datetime, timedelta

from scripts.get_engine import *


# ---------------------------------------------------------
# TASK: EXTRACT DATA FROM API 
# ---------------------------------------------------------
def extract_data_from_api(**kwargs):
  """Extract Data API dengan pagination dan metode Append ke File (Format JSONL)."""
  
  api_url = kwargs.get("api_url")
  limit = kwargs.get("limit")
  file_name = kwargs.get("file_name")
  filter_field = kwargs.get("filter_key")
  last_watermark = kwargs.get("last_watermark")
  lookback_days = kwargs.get("lookback_days", 0)
  params = {"limit": limit}

  logger.info("=" * 150)
  if filter_field and last_watermark:
    base_dt = datetime.strptime(last_watermark, "%Y-%m-%d")
    effective_dt = base_dt - timedelta(days=lookback_days)
    params[filter_field] = effective_dt.strftime("%Y-%m-%dT00:00:00")
    logger.info(
        "Applying incremental filter: %s >= %s (lookback: %d days)",
        filter_field,
        params[filter_field],
        lookback_days,
    )

  # Create directory for raw data
  raw_dir = Path("/opt/airflow/logs/raw_data")
  raw_dir.mkdir(parents=True, exist_ok=True)
  file_path = raw_dir / f"{file_name}.jsonl"

  if file_path.exists():
    file_path.unlink()

  total_fetched = 0
  offset = 0
  page = 1

  logger.info("Starting extraction from API: %s", api_url)

  try:

    with open(file_path, "a", encoding="utf-8") as f:
      while True:
        params["offset"] = offset
        logger.info("Fetching page %d (offset=%d, limit=%d)", page, offset, limit)

        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()

        result_json = response.json()
        page_data = result_json.get("data", [])

        if not page_data:
          break
        
        # Write JSON records in JSONL format
        for item in page_data:
          f.write(json.dumps(item, ensure_ascii=False) + "\n")

        total_fetched += len(page_data)

        if len(page_data) < limit:
          break

        offset += limit
        page += 1
        logger.info("=" * 150)

    logger.info(
        "Extraction completed: %d rows written to %s across %d page(s)",
        total_fetched,
        file_path,
        page,
    )
    logger.info("=" * 150)

    # Push filepath string into XCOM
    kwargs["ti"].xcom_push(key="data_json", value=str(file_path))
    kwargs["ti"].xcom_push(key="data_name", value=str(file_name))
    return str(file_path)

  except requests.exceptions.RequestException as e:
    logger.error("Extraction failed for %s: %s", api_url, e)
    raise

# ---------------------------------------------------------
# TASK: VALIDATOR DATA FROM API 
# ---------------------------------------------------------
DATASET_SCHEMAS = {
    "master_employee": [
        "npp",
        "nama",
        "level",
        "grade",
        "jabatan",
        "unit_kerja",
        "gender",
        "updated_at",
    ],
    "master_provider": [
        "provider_id",
        "learning_provider",
        "lokasi_kegiatan",
        "updated_at",
    ],
    "master_competency": [
        "kode_komp",
        "nama_kompetensi",
        "learning_academy",
        "sub_learning_academy",
        "standard_point",
        "updated_at",
    ],
    "master_unit": [
        "unit_code",
        "unit_kerja",
        "annual_budget_plafond",
        "updated_at",
    ],
    "fact_activity": [
        "activity_id",
        "nama_kegiatan",
        "jenis_pembelajaran",
        "category",
        "type",
        "type_event",
        "learning_academy",
        "sub_learning_academy",
        "kode_komp",
        "npp",
        "nama",
        "level",
        "grade",
        "jabatan",
        "unit_kerja",
        "gender",
        "status_kegiatan",
        "type_narasumber",
        "type_mc",
        "point",
        "total_poin",
        "tanggal_event",
        "jam_mulai",
        "jam_selesai",
        "kat_sertifikasi",
        "inisiator_unit",
        "learning_provider",
        "lokasi_kegiatan",
        "biaya_rab_ldd",
        "budget_realisasi_rab_led",
        "biaya_penginapan",
        "biaya_transportasi",
        "biaya_konsumsi",
        "biaya_training_kit",
        "biaya_fasilitas_pendukung",
        "total_anggaran",
        "purchase_number",
        "purchase_date",
        "npp_pic",
        "nama_pic",
        "insert_at",
    ],
}

def validate_data_from_api(**kwargs):
    """Validate JSONL schema and format records to string values."""

    # Retrieve execution arguments from task instance kwargs
    extract_task_id = kwargs["extract_task_id"]
    task_group_id = kwargs["task_group_id"]
    data_name = kwargs["data_name"]
    raw_path = kwargs["ti"].xcom_pull(
        task_ids=f"{task_group_id}.{extract_task_id}",
        key="data_json",
    )
    
    # Verify dataset schema existence
    logger.info("=" * 150)
    schema = DATASET_SCHEMAS.get(data_name)
    if schema is None:
        raise ValueError(
            f"Schema for dataset '{data_name}' not found. "
            f"Available datasets: {list(DATASET_SCHEMAS)}"
        )

    # Validate presence of raw file path from upstream task
    if not raw_path:
        raise ValueError(
            f"Raw file path not found from upstream task '{extract_task_id}'."
        )

    # Validate file existence on disk
    file_path = Path(raw_path)
    if not file_path.is_file():
        raise FileNotFoundError(
            f"Raw extraction file not found: {file_path}"
        )

    logger.info("Validating dataset '%s' from %s", data_name, file_path)

    # Prepare destination directory and remove existing validated file
    validated_dir = Path("/opt/airflow/logs/validated_data")
    validated_dir.mkdir(parents=True, exist_ok=True)
    validated_path = validated_dir / f"{data_name}.jsonl"

    if validated_path.exists():
        validated_path.unlink()

    # Handle empty raw file without processing chunks
    if file_path.stat().st_size == 0:
        logger.warning("Raw file is empty for dataset '%s'.", data_name)
        validated_path.touch()
        kwargs["ti"].xcom_push(key="data_validated", value=str(validated_path))
        return str(validated_path)

    expected_columns = set(schema)
    total_validated_rows = 0
    chunk_index = 0
    chunk_size = 1000

    try:
        with validated_path.open("w", encoding="utf-8") as out_file:
            # Process records in 1000-row chunks to prevent out-of-memory errors
            for chunk in pd.read_json(
                file_path,
                lines=True,
                chunksize=chunk_size,
                convert_dates=False,
                dtype=False,
            ):
                # Skip empty chunks
                if chunk.empty:
                    continue

                logger.info("Processing validation chunk %d (%d rows)", chunk_index + 1, len(chunk))

                # Validate expected and unexpected columns on the first chunk
                if chunk_index == 0:
                    actual_columns = set(chunk.columns)
                    missing_columns = expected_columns - actual_columns
                    if missing_columns:
                        raise ValueError(
                            f"Dataset '{data_name}' is missing required columns: {sorted(missing_columns)}"
                        )

                    unexpected_columns = actual_columns - expected_columns
                    if unexpected_columns:
                        logger.warning(
                            "Dataset '%s' contains unexpected columns: %s",
                            data_name,
                            sorted(unexpected_columns),
                        )

                # Convert all columns to string representation while preserving nulls
                chunk = chunk.where(pd.notna(chunk), None)
                for column in chunk.columns:
                    chunk[column] = chunk[column].map(
                        lambda value: str(value) if value is not None else None
                    )

                # Write chunk records to the validated file in JSONL format
                for record in chunk.to_dict(orient="records"):
                    out_file.write(
                        json.dumps(
                            record,
                            ensure_ascii=False,
                            allow_nan=False,
                        ) + "\n"
                    )

                total_validated_rows += len(chunk)
                chunk_index += 1
                logger.info(
                    "Completed chunk %d: %d rows written (cumulative: %d rows)",
                    chunk_index,
                    len(chunk),
                    total_validated_rows,
                )
                logger.info("=" * 150)

        logger.info(
            "Validation completed: dataset=%s | total_rows=%d | output=%s",
            data_name,
            total_validated_rows,
            validated_path,
        )
        logger.info("=" * 150)

    except Exception as e:
        logger.error(
            "Validation failed for dataset '%s': %s",
            data_name,
            e,
        )
        raise

    # Push validated file path into XCom
    kwargs["ti"].xcom_push(key="data_validated", value=str(validated_path))
    return str(validated_path)
   
# ---------------------------------------------------------
# TASK: INGEST DATA INTO DATAWAREHOUSE 
# ---------------------------------------------------------
def ingest_data_into_datawarehouse(**kwargs) -> int:
    """Load validated JSONL into staging (datawarehouse)."""

    logger.info("=" * 150)
    # Retrieve execution arguments from task instance kwargs
    validate_task_id = kwargs["validate_task_id"]
    validate_group_id = kwargs["validate_group_id"]
    table_name = kwargs["table_name"]

    validated_path_str = kwargs["ti"].xcom_pull(
        task_ids=f"{validate_group_id}.{validate_task_id}",
        key="data_validated",
    )

    if not validated_path_str:
        raise ValueError(
            f"Validated data path not received from task '{validate_group_id}.{validate_task_id}'."
        )

    file_path = Path(validated_path_str)
    if not file_path.is_file():
        raise FileNotFoundError(f"Validated JSONL file not found: {file_path}")

    # Handle empty file
    if file_path.stat().st_size == 0:
        logger.warning(
            "Validated file %s is empty. Skipping database ingestion for table '%s'.",
            file_path,
            table_name,
        )
        kwargs["ti"].xcom_push(key="rows_ingested", value=0)
        return 0

    # Get engine for data warehouse
    engine = db_warehouse_engine()

    chunk_size = 5000
    chunk_index = 0
    total_ingested_rows = 0

    logger.info(
        "Starting ingestion from %s into staging.%s (chunk_size=%d)...",
        file_path,
        table_name,
        chunk_size,
    )

    try:
        for chunk in pd.read_json(
            file_path,
            lines=True,
            chunksize=chunk_size,
            convert_dates=False,
            dtype=False,
        ):
            if chunk.empty:
                continue

            chunk_index += 1
            chunk_rows = len(chunk)
            logger.info(
                "Ingesting chunk %d (%d rows) into staging.%s...",
                chunk_index,
                chunk_rows,
                table_name,
            )

            # Insert chunk into PostgreSQL staging schema
            with engine.begin() as conn:
                chunk.to_sql(
                    name=table_name,
                    con=conn,
                    schema="staging",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )

            total_ingested_rows += chunk_rows
            logger.info(
                "Completed chunk %d: %d rows loaded (cumulative: %d rows) into staging.%s",
                chunk_index,
                chunk_rows,
                total_ingested_rows,
                table_name,
            )
            logger.info("=" * 150)

        logger.info(
            "Ingestion completed successfully: total %d rows inserted into staging.%s from %s",
            total_ingested_rows,
            table_name,
            file_path,
        )
        logger.info("=" * 150)

        kwargs["ti"].xcom_push(key="rows_ingested", value=total_ingested_rows)
        return total_ingested_rows

    except Exception as e:
        logger.error(
            "Database ingestion failed for table 'staging.%s' from file '%s'. Error: %s",
            table_name,
            file_path,
            str(e),
            exc_info=True,
        )
        raise
        
    

