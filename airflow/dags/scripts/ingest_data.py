import requests
import logging
logger = logging.getLogger("airflow.task")
import pandas as pd
from pathlib import Path
import json
from datetime import datetime, timedelta

# ---------------------------------------------------------
# TASK: EXTRACT DATA FROM API 
# ---------------------------------------------------------
def extract_data_from_api(**kwargs) -> str:
  """Extract Data API dengan pagination dan metode Append ke File (Format JSONL)."""
  
  api_url = kwargs.get("api_url")
  limit = kwargs.get("limit")
  file_name = kwargs.get("file_name")
  filter_field = kwargs.get("filter_key")
  last_watermark = kwargs.get("last_watermark")
  lookback_days = kwargs.get("lookback_days", 0)
  params = {"limit": limit}

  if filter_field and last_watermark:
    base_dt = datetime.strptime(last_watermark, "%Y-%m-%d")
    effective_dt = base_dt - timedelta(days=lookback_days)
    params[filter_field] = effective_dt.strftime("%Y-%m-%dT00:00:00")
    logger.info(f"Incremental Filter: {filter_field} >= {params[filter_field]} (Lookback: {lookback_days} days)")

  # create directory for raw data
  raw_dir = Path("/opt/airflow/logs/raw_data")
  raw_dir.mkdir(parents=True, exist_ok=True)
  file_path = raw_dir / f"{file_name}.jsonl"

  if file_path.exists():
    file_path.unlink()

  total_fetched = 0 # show total fetched records
  offset = 0 # pagination config
  page = 1 # page counter

  logger.info("=" * 150)
  logger.info(f"🔹 START APPEND EXTRACTION: [{api_url}]")

  try:

    with open(file_path, "a", encoding="utf-8") as f:
      # start loop for pagination
      while True:
        params["offset"] = offset # add offset into params
        logger.info(f"-> Fetching page {page} with params: {params}")

        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()

        result_json = response.json()
        page_data = result_json.get("data", [])

        # Break there is not page_data
        if not page_data:
          break
        
        # Write JSON (append method)
        for item in page_data:
          f.write(json.dumps(item, ensure_ascii=False) + "\n")

        total_fetched += len(page_data)

        if len(page_data) < limit:
          break

        offset += limit
        page += 1

    logger.info(
        f"✅ SUCCESS: Appended total {total_fetched} rows across {page}"
        f" page(s) to {file_path}"
    )
    logger.info("=" * 150)

    # Push filepath string into XCom
    kwargs["ti"].xcom_push(key="data_json", value=str(file_path))
    kwargs["ti"].xcom_push(key="data_name", value=str(file_name))
    return str(file_path)

  except requests.exceptions.RequestException as e:
    logger.error(f"❌ ERROR: Failed during append extraction. Details: {e}")
    logger.info("=" * 50)
    raise

# ---------------------------------------------------------
# TASK: VALIDATOR DATA FROM API 
# ---------------------------------------------------------
DATASET_SCHEMAS = {
    "master_employee": {
        "npp": "String",
        "nama": "String",
        "level": "String",
        "grade": "String",
        "jabatan": "String",
        "unit_kerja": "String",
        "gender": "String",
        "updated_at": "DateTime",
    },
    "master_provider": {
        "provider_id": "String",
        "learning_provider": "String",
        "lokasi_kegiatan": "String",
        "updated_at": "DateTime",
    },
    "master_competency": {
        "kode_komp": "String",
        "nama_kompetensi": "String",
        "learning_academy": "String",
        "sub_learning_academy": "String",
        "standard_point": "UInt64",
        "updated_at": "DateTime",
    },
    "master_unit": {
        "unit_code": "String",
        "unit_kerja": "String",
        "annual_budget_plafond": "UInt64",
        "updated_at": "DateTime",
    },
    "fact_activity": {
        "activity_id": "String",
        "nama_kegiatan": "String",
        "jenis_pembelajaran": "String",
        "category": "String",
        "type": "String",
        "type_event": "String",
        "learning_academy": "String",
        "sub_learning_academy": "String",
        "kode_komp": "String",
        "npp": "String",
        "nama": "String",
        "level": "String",
        "grade": "String",
        "jabatan": "String",
        "unit_kerja": "String",
        "gender": "String",
        "status_kegiatan": "String",
        "type_narasumber": "String",
        "type_mc": "String",
        "point": "UInt64",
        "total_poin": "UInt64",
        "tanggal_event": "Date",
        "jam_mulai": "Time",
        "jam_selesai": "Time",
        "kat_sertifikasi": "String",
        "inisiator_unit": "String",
        "learning_provider": "String",
        "lokasi_kegiatan": "String",
        "biaya_rab_ldd": "UInt64",
        "budget_realisasi_rab_led": "UInt64",
        "biaya_penginapan": "UInt64",
        "biaya_transportasi": "UInt64",
        "biaya_konsumsi": "UInt64",
        "biaya_training_kit": "UInt64",
        "biaya_fasilitas_pendukung": "UInt64",
        "total_anggaran": "UInt64",
        "purchase_number": "String",
        "purchase_date": "Date",
        "npp_pic": "String",
        "nama_pic": "String",
        "insert_at": "DateTime",
    },
}

NULL_VALUES = {"", "-", "N/A", "n/a", "NULL", "Null", "NaN", "nan"}

def validate_data_from_api(**kwargs) -> str:
    """Validasi data JSONL berdasarkan schema dataset."""

    extract_task_id = kwargs["extract_task_id"]
    task_group_id = kwargs["task_group_id"]
    data_name = kwargs["data_name"]

    schema = DATASET_SCHEMAS.get(data_name)
    if schema is None:
        raise ValueError(
            f"Schema untuk dataset '{data_name}' tidak ditemukan. "
            f"Dataset tersedia: {list(DATASET_SCHEMAS)}"
        )

    raw_path = kwargs["ti"].xcom_pull(
        task_ids=f"{task_group_id}.{extract_task_id}",
        key="data_json",
    )

    if not raw_path:
        raise ValueError(
            f"Path file hasil extraction tidak ditemukan dari "
            f"task '{extract_task_id}'."
        )

    file_path = Path(raw_path)

    if not file_path.is_file():
        raise FileNotFoundError(
            f"File hasil extraction tidak ditemukan: {file_path}"
        )

    logger.info("Starting validation for dataset: %s", data_name)

    clean_data = pd.read_json(file_path, lines=True)

    if clean_data.empty:
        logger.warning("Dataset '%s' tidak memiliki data.", data_name)

    expected_columns = set(schema)
    actual_columns = set(clean_data.columns)

    missing_columns = expected_columns - actual_columns
    unexpected_columns = actual_columns - expected_columns

    if missing_columns:
        raise ValueError(
            f"Dataset '{data_name}' kehilangan kolom wajib: "
            f"{sorted(missing_columns)}"
        )

    if unexpected_columns:
        logger.warning(
            "Dataset '%s' memiliki kolom yang tidak terdaftar: %s",
            data_name,
            sorted(unexpected_columns),
        )

    string_columns = [
        column
        for column, data_type in schema.items()
        if data_type == "String"
    ]

    for column in string_columns:
        clean_data[column] = clean_data[column].map(
            lambda value: (
                None
                if pd.isna(value)
                else (
                    None
                    if isinstance(value, str)
                    and value.strip() in NULL_VALUES
                    else (
                        value.strip()
                        if isinstance(value, str)
                        else value
                    )
                )
            )
        )

    for column, data_type in schema.items():
        if data_type == "String":
            invalid_mask = clean_data[column].map(
                lambda value: (
                    not pd.isna(value)
                    and not isinstance(value, str)
                )
            )
            if invalid_mask.any():
                raise ValueError(
                    f"Kolom '{column}' pada '{data_name}' "
                    "mengandung nilai non-string."
                )

        elif data_type == "UInt64":
            numeric = pd.to_numeric(
                clean_data[column], errors="coerce"
            )

            invalid_mask = (
                numeric.isna()
                & clean_data[column].notna()
            )

            invalid_mask |= (
                numeric.notna()
                & (
                    (numeric < 0)
                    | (numeric % 1 != 0)
                    | (numeric > 18446744073709551615)
                )
            )

            if invalid_mask.any():
                raise ValueError(
                    f"Kolom '{column}' pada '{data_name}' "
                    "mengandung nilai yang bukan UInt64 valid."
                )

            clean_data[column] = numeric.astype("UInt64")

        elif data_type == "DateTime":
            original = clean_data[column]
            parsed = pd.to_datetime(
                original, errors="coerce", utc=True
            )

            invalid_mask = parsed.isna() & original.notna()

            if invalid_mask.any():
                raise ValueError(
                    f"Kolom '{column}' pada '{data_name}' "
                    "mengandung nilai DateTime yang tidak valid."
                )

            clean_data[column] = parsed

        elif data_type == "Date":
            original = clean_data[column]
            parsed = pd.to_datetime(original, errors="coerce")
            invalid_mask = parsed.isna() & original.notna()
            if invalid_mask.any():
                raise ValueError(
                    f"Kolom '{column}' pada '{data_name}' "
                    "mengandung nilai Date yang tidak valid."
                )
            clean_data[column] = parsed.dt.strftime("%Y-%m-%d")
        
        elif data_type == "Time":
            original = clean_data[column]
            parsed = pd.to_datetime(original, format="%H:%M:%S", errors="coerce")
            invalid_mask = parsed.isna() & original.notna()
            if invalid_mask.any():
                raise ValueError(
                    f"Kolom '{column}' pada '{data_name}' "
                    "mengandung nilai Time yang tidak valid."
                )
            clean_data[column] = parsed.dt.strftime("%H:%M:%S")

        else:
            raise ValueError(
                f"Tipe data '{data_type}' belum didukung "
                f"untuk kolom '{column}'."
            )

    rows_before = len(clean_data)
    clean_data = clean_data.drop_duplicates()
    duplicates_removed = rows_before - len(clean_data)

    validated_dir = Path("/opt/airflow/logs/validated_data")
    validated_dir.mkdir(parents=True, exist_ok=True)

    validated_path = validated_dir / f"{data_name}.jsonl"

    output_data = clean_data.copy()

    for column, data_type in schema.items():
        if data_type == "DateTime":
            output_data[column] = output_data[column].map(
                lambda value: (
                    value.isoformat()
                    if not pd.isna(value)
                    else None
                )
            )

    with validated_path.open("w", encoding="utf-8") as file:
        for record in output_data.to_dict(orient="records"):
            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    allow_nan=False,
                ) + "\n"
            )

    logger.info(
        "Validation successful | dataset=%s | rows=%s | "
        "duplicates_removed=%s | output=%s",
        data_name,
        len(clean_data),
        duplicates_removed,
        validated_path,
    )


# ---------------------------------------------------------
# TASK: INGEST DATA INTO CLICKHOUSE 
# ---------------------------------------------------------
def ingest_data_into_clickhouse(**kwargs) -> pd.DataFrame:
    """Ingest Data Into Clickhouse, Use Batch Insert"""
    pass

