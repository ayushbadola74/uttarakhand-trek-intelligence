from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def ingestion_task():
    print("Fetching trek and weather data...")


def processing_task():
    print("Processing data with PySpark...")


def dbt_task():
    print("Running dbt transformations...")


def testing_task():
    print("Running dbt tests...")


with DAG(
    dag_id="uttarakhand_trek_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["uttarakhand", "data-engineering"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=ingestion_task,
    )

    process = PythonOperator(
        task_id="process_data",
        python_callable=processing_task,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=dbt_task,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=testing_task,
    )

    ingest >> process >> dbt_run >> dbt_test