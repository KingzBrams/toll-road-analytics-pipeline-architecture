from datetime import datetime
from airflow import DAG

# pyrefly: ignore[missing-import]
from airflow.operators.empty import EmptyOperator
# pyrefly: ignore[missing-import]
from airflow.operators.python import PythonOperator
# pyrefly: ignore[missing-import]
from airflow.utils.task_group import TaskGroup
# pyrefly: ignore[missing-import]
from airflow.utils.trigger_rule import TriggerRule

from scripts.ingest_data import *

with DAG(
    dag_id="toll_road_analytics_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

  start_task = EmptyOperator(task_id="start_pipeline")
  end_task = EmptyOperator(task_id="end_pipeline")

  # ==========================================
  # 1. TASKGROUP: MASTER EMPLOYEES
  # ==========================================
  with TaskGroup(
      "ingest_data_master_employee", tooltip="Ingest Master Employees"
  ) as tg_employees:
    extract_employee = PythonOperator(
        task_id="extract_employee_task",
        python_callable=extract_data_from_api,
        op_kwargs={
            "api_url": "http://data-gen-api:8000/api/v1/masters/employees",
            "limit": 1000,
        },
    )


  # ==========================================
  # 2. TASKGROUP: MASTER PROVIDERS
  # ==========================================
  with TaskGroup(
      "ingest_data_master_provider", tooltip="Ingest Master Providers"
  ) as tg_providers:
    extract_provider = PythonOperator(
        task_id="extract_provider_task",
        python_callable=extract_data_from_api,
        op_kwargs={
            "api_url": "http://data-gen-api:8000/api/v1/masters/providers",
            "limit": 1000,
        },
    )

  # ==========================================
  # 3. TASKGROUP: MASTER COMPETENCIES
  # ==========================================
  with TaskGroup(
      "ingest_data_master_competency", tooltip="Ingest Master Competencies"
  ) as tg_competencies:
    extract_competency = PythonOperator(
        task_id="extract_competency_task",
        python_callable=extract_data_from_api,
        op_kwargs={
            "api_url": "http://data-gen-api:8000/api/v1/masters/competencies",
            "limit": 1000,
        },
    )

  # ==========================================
  # 4. TASKGROUP: MASTER UNITS
  # ==========================================
  with TaskGroup(
      "ingest_data_master_unit", tooltip="Ingest Master Units/Cost Centers"
  ) as tg_units:
    extract_unit = PythonOperator(
        task_id="extract_unit_task",
        python_callable=extract_data_from_api,
        op_kwargs={
            "api_url": "http://data-gen-api:8000/api/v1/masters/units",
            "limit": 1000,
        },
    )

  # ==========================================
  # 5. TASKGROUP: FACT TRAINING ACTIVITIES (Incremental)
  # ==========================================
  with TaskGroup(
      "ingest_data_fact_activities", tooltip="Ingest Fact Training Activities"
  ) as tg_activities:
  
    extract_activities = PythonOperator(
        task_id="extract_activities_task",
        python_callable=extract_data_from_api,
        op_kwargs={
            "api_url": "http://data-gen-api:8000/api/v1/facts/training-activities",
            "last_watermark": ( 
                "2026-10-01T00:00:00"
            ),
            "limit": 5000,
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

  # ==========================================
  # ORKESTRASI DAG (DEPENDENCIES)
  # ==========================================
  start_task >> [
      tg_employees,
      tg_providers,
      tg_competencies,
      tg_units,
  ] >> tg_activities >> end_task