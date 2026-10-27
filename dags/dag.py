"""Airflow DAG for the POCC fleet realization ETL pipeline.

All schedule-related values are read from environment variables so the DAG
behaviour can be changed without touching this file. No database work happens
at parse time: the module only imports the task callables.
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.config.settings import env_bool, env_int
from src.main import check_connections, run_pipeline

DEFAULT_DAG_ID = "mart_pocc_fleet_realization"
DEFAULT_TIMEZONE = "Asia/Jakarta"
DEFAULT_START_DATE = "2026-09-08T15:00:00+07:00"
DEFAULT_INTERVAL_MINUTES = 3


def _build_schedule() -> str:
    """Prefer an explicit cron; otherwise derive one from the interval minutes."""
    explicit_schedule = os.environ.get("DAG_SCHEDULE", "").strip()
    if explicit_schedule:
        return explicit_schedule
    interval_minutes = env_int("ETL_INTERVAL_MINUTES", DEFAULT_INTERVAL_MINUTES)
    if interval_minutes < 1:
        raise ValueError("ETL_INTERVAL_MINUTES must be at least 1")
    return f"*/{interval_minutes} * * * *"


def _build_start_date():
    """Return a static, timezone-aware start date from configuration."""
    timezone = pendulum.timezone(
        os.environ.get("DAG_TIMEZONE", DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    )
    start_date = os.environ.get("ETL_START_TIME", "").strip() or DEFAULT_START_DATE
    return pendulum.parse(start_date).in_timezone(timezone)


dag = DAG(
    dag_id=os.environ.get("DAG_ID", "").strip() or DEFAULT_DAG_ID,
    description=(
        "Extract ferry fleet realization data from the BRONZE Oracle database, "
        "transform it with Polars, and merge it into the POCC mart table."
    ),
    schedule=_build_schedule(),
    start_date=_build_start_date(),
    catchup=env_bool("DAG_CATCHUP", False),
    max_active_runs=env_int("DAG_MAX_ACTIVE_RUNS", 1),
    is_paused_upon_creation=env_bool("DAG_IS_PAUSED", False),
    default_args={
        "retries": env_int("DAG_RETRIES", 1),
        "retry_delay": timedelta(
            minutes=env_int("DAG_RETRY_DELAY_MINUTES", 1)
        ),
        "execution_timeout": timedelta(
            minutes=env_int("DAG_EXECUTION_TIMEOUT_MINUTES", 10)
        ),
    },
    tags=["etl", "pocc", "oracle", "polars"],
)

validate_connections_task = PythonOperator(
    task_id="validate_connections",
    python_callable=check_connections,
    dag=dag,
)

run_etl_task = PythonOperator(
    task_id="run_etl",
    python_callable=run_pipeline,
    dag=dag,
)

validate_connections_task >> run_etl_task
