from datetime import datetime
import subprocess

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def run_script(script_path):
    print(f"Running: {script_path}")

    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Script failed with exit code {result.returncode}"
        )


def ingestion_task():
    run_script(
        "/opt/airflow/pipeline/ingestion/trek_ingestion.py"
    )

    run_script(
        "/opt/airflow/pipeline/ingestion/weather_ingestion.py"
    )


def processing_task():
    print("PySpark processing started...")
    print("Processing will run in the next pipeline stage.")


def dbt_task():
    print("dbt transformation started...")


def testing_task():
    print("dbt tests started...")


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