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
    # schedule="@daily",
    schedule=None,
    catchup=False,
    max_active_runs=1
) as dag:

    start_task = EmptyOperator(task_id="start_pipeline")
    end_task = EmptyOperator(task_id="end_pipeline", trigger_rule=TriggerRule.ALL_DONE,)

  # ==========================================
  # TASKGROUP: MASTER EMPLOYEES
  # ==========================================
    with TaskGroup("ingest_data_master_employee", tooltip="Ingest Master Employees") as tg_employees:

        extract_employee = PythonOperator(
            task_id="extract_employee_task",
            python_callable=extract_data_from_api,
            op_kwargs={
                "api_url": "http://data-gen-api:8000/api/v1/masters/employees",
                "limit": 1000,
                "last_watermark": "{{ ds }}",
                "filter_key": "updated_at",
                "file_name": "employee"
            },
        )
    
        validate_employee = PythonOperator(
            task_id="validate_employee_task",
            python_callable=validate_data_from_api,
            op_kwargs={
                "extract_task_id": "extract_employee_task",
                "task_group_id": "ingest_data_master_employee",
                "data_name": "master_employee",
            },
        )

    extract_employee >> validate_employee


    # ==========================================
    # TASKGROUP: MASTER PROVIDERS
    # ==========================================
    with TaskGroup("ingest_data_master_provider", tooltip="Ingest Master Providers") as tg_providers:

        extract_provider = PythonOperator(
            task_id="extract_provider_task",
            python_callable=extract_data_from_api,
            op_kwargs={
                "api_url": "http://data-gen-api:8000/api/v1/masters/providers",
                "limit": 1000,
                "last_watermark": "{{ ds }}",
                "filter_key": "updated_at",
                "file_name": "provider"
            },
        )

        validate_provider = PythonOperator(
            task_id="validate_provider_task",
            python_callable=validate_data_from_api,
            op_kwargs={
                "extract_task_id": "extract_provider_task",
                "task_group_id": "ingest_data_master_provider",
                "data_name": "master_provider",
            },
        )

    extract_provider >> validate_provider

    # ==========================================
    # TASKGROUP: MASTER COMPETENCIES
    # ==========================================
    with TaskGroup("ingest_data_master_competency", tooltip="Ingest Master Competencies") as tg_competencies:

        extract_competency = PythonOperator(
            task_id="extract_competency_task",
            python_callable=extract_data_from_api,
            op_kwargs={
                "api_url": "http://data-gen-api:8000/api/v1/masters/competencies",
                "limit": 1000,
                "last_watermark": "{{ ds }}",
                "filter_key": "updated_at",
                "file_name": "competency"
            },
        )

        validate_competency = PythonOperator(
            task_id="validate_competency_task",
            python_callable=validate_data_from_api,
            op_kwargs={
                "extract_task_id": "extract_competency_task",
                "task_group_id": "ingest_data_master_competency",
                "data_name": "master_competency",
            },
        )

    extract_competency >> validate_competency

    # ==========================================
    # TASKGROUP: MASTER UNITS
    # ==========================================
    with TaskGroup("ingest_data_master_unit", tooltip="Ingest Master Units/Cost Centers") as tg_units:

        extract_unit = PythonOperator(
            task_id="extract_unit_task",
            python_callable=extract_data_from_api,
            op_kwargs={
                "api_url": "http://data-gen-api:8000/api/v1/masters/units",
                "limit": 1000,
                "last_watermark": "{{ ds }}",
                "filter_key": "updated_at",
                "file_name": "unit"
            },
        )

        validate_unit = PythonOperator(
            task_id="validate_unit_task",
            python_callable=validate_data_from_api,
            op_kwargs={
                "extract_task_id": "extract_unit_task",
                "task_group_id": "ingest_data_master_unit",
                "data_name": "master_unit",
            },
        )

    extract_unit >> validate_unit

    # ==========================================
    # TASKGROUP: FACT TRAINING ACTIVITIES (Incremental)
    # ==========================================
    with TaskGroup("ingest_data_fact_activities", tooltip="Ingest Fact Training Activities") as tg_activities:

        extract_activities = PythonOperator(
            task_id="extract_activities_task",
            python_callable=extract_data_from_api,
            op_kwargs={
                "api_url": "http://data-gen-api:8000/api/v1/facts/training-activities",
                "limit": 1000,
                "file_name": "activity",
            },
        )

        validate_activity = PythonOperator(
            task_id="validate_activity_task",
            python_callable=validate_data_from_api,
            op_kwargs={
                "extract_task_id": "extract_activities_task",
                "task_group_id": "ingest_data_fact_activities",
                "data_name": "fact_activity",
            },
        )

    extract_activities >> validate_activity

    # ==========================================
    # ORKESTRASI DAG (DEPENDENCIES)
    # ==========================================
    start_task >> [
        tg_employees,
        tg_providers,
        tg_competencies,
        tg_units,
        tg_activities
    ] >> end_task