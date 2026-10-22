from datetime import datetime, timedelta

from airflow import DAG 
# pyrefly: ignore[missing-import]
from airflow.utils.task_group import TaskGroup
# pyrefly: ignore[missing-import]
from airflow.operators.empty import EmptyOperator 
# pyrefly: ignore[missing-import]
from airflow.operators.python import PythonOperator 

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG (
    dag_id='toll_road_analytics_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['toll_road_analytics']
) as dag:

    start_task = EmptyOperator(task_id="start_pipeline")

    start_task

