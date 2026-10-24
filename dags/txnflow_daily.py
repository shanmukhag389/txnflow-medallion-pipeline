"""Daily TxnFlow pipeline: generate -> bronze -> silver -> gold."""
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = os.environ.get("TXNFLOW_HOME", os.path.expanduser("~/txnflow-medallion-pipeline"))
PYTHON = f"{PROJECT}/.venv/bin/python"


def run(script):
    # {{ ds }} is the run's date (YYYY-MM-DD); Airflow fills it in at runtime.
    return f"cd {PROJECT} && {PYTHON} {script} --date " + "{{ ds }}"


with DAG(
    dag_id="txnflow_daily",
    description="Generate raw data, then bronze, silver and gold for one day",
    start_date=datetime(2026, 10, 22),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["txnflow"],
) as dag:
    generate = BashOperator(
        task_id="generate_raw", bash_command=run("generator/generate_transactions.py"))
    bronze = BashOperator(task_id="bronze", bash_command=run("spark_jobs/bronze.py"))
    silver = BashOperator(task_id="silver", bash_command=run("spark_jobs/silver.py"))
    gold = BashOperator(task_id="gold", bash_command=run("spark_jobs/gold.py"))

    generate >> bronze >> silver >> gold
