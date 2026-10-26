"""Daily TxnFlow pipeline: generate -> bronze -> silver -> gold -> snowflake."""
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = os.environ.get("TXNFLOW_HOME", os.path.expanduser("~/txnflow-medallion-pipeline"))
# Main virtual env for generators/spark
PYTHON = f"{PROJECT}/.venv/bin/python"
# Dedicated Airflow virtual env containing the snowflake-connector library
AIRFLOW_PYTHON = f"{os.path.expanduser('~')}/.venv-airflow/bin/python"

def run(script, python_path=PYTHON):
    # {{ ds }} is the run's date (YYYY-MM-DD); Airflow fills it in at runtime.
    return f"cd {PROJECT} && {python_path} {script} --date " + "{{ ds }}"

with DAG(
    dag_id="txnflow_daily",
    description="Generate raw data, process Medallion layers, and load to Snowflake",
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
    
    # Task 5: Land the Gold Parquet aggregates directly into Snowflake tables
    load_snowflake = BashOperator(
        task_id="load_snowflake", 
        bash_command=run("spark_jobs/load_snowflake.py", python_path=AIRFLOW_PYTHON)
    )

    generate >> bronze >> silver >> gold >> load_snowflake
