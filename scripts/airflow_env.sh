# Usage: source scripts/airflow_env.sh
export TXNFLOW_HOME="$HOME/txnflow-medallion-pipeline"
export AIRFLOW_HOME="$HOME/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$TXNFLOW_HOME/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
source "$HOME/.venv-airflow/bin/activate"
