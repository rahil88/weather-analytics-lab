"""
Weather dbt: runs the dbt project in /opt/airflow/dbt after the ETL finishes.

Scheduling: `schedule=[WEATHER_DAILY_DATASET]` makes this DAG run automatically each
time weather_etl's load task updates DEMO_DB.RAW.WEATHER_DAILY, so dbt always
runs after the ETL.

Credentials: dbt/profiles.yml reads only environment variables. We fill them here
from the Airflow Connection "snowflake_conn", so no secret is stored in the dbt project.
"""
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_FLAGS = f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
WEATHER_DAILY_DATASET = Dataset("snowflake://DEMO_DB/RAW/WEATHER_DAILY")

conn = BaseHook.get_connection("snowflake_conn")
dbt_env = {
    "DBT_USER": conn.login or "",
    "DBT_ACCOUNT": conn.extra_dejson.get("account", ""),
    "DBT_ROLE": conn.extra_dejson.get("role", ""),
    "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse", ""),
    "DBT_DATABASE": conn.extra_dejson.get("database", ""),
    "DBT_PRIVATE_KEY_PATH": conn.extra_dejson.get("private_key_file", ""),
    "DBT_SCHEMA": "ANALYTICS",
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
}

with DAG(
    dag_id="weather_dbt",
    description="dbt run / test / snapshot on the weather data, triggered after weather_etl",
    start_date=datetime(2026, 9, 28),
    schedule=[WEATHER_DAILY_DATASET],
    catchup=False,
    default_args={"env": dbt_env},
    tags=["ELT", "dbt", "weather"],
) as dag:
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"dbt run {DBT_FLAGS}")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"dbt test {DBT_FLAGS}")
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=f"dbt snapshot {DBT_FLAGS}")

    dbt_run >> dbt_test >> dbt_snapshot
