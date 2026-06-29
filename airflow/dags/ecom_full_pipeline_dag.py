"""
airflow/dags/ecom_full_pipeline_dag.py

Main DAG for the Olist E-Commerce data pipeline.

Schedule: daily at 02:00 UTC (09:00 VN time)
Retries: 2 retries with 5-minute exponential backoff

Task flow:
  1. ingest_olist_to_s3        — Download Olist CSVs → upload to S3
  2. load_staging_to_redshift  — COPY from S3 → Redshift staging
  3. dbt_run_staging           — dbt run --select staging
  4. dbt_test_staging          — dbt test --select staging (quality gate)
  5. dbt_run_mart              — dbt run --select mart
  6. dbt_test_mart             — dbt test --select mart
  7. notify_success            — log completion (extend to Slack/email)
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)
 
# DAG DEFAULT ARGS
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

DBT_DIR = "/usr/app/dbt_project"
DBT_CMD = f"cd {DBT_DIR} && dbt"
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "DBT_TARGET": "prod",
    **{k: os.environ.get(k, "") for k in [
        "REDSHIFT_HOST", "REDSHIFT_PORT", "REDSHIFT_DATABASE",
        "REDSHIFT_USER", "REDSHIFT_PASSWORD",
    ]},
}

# PYTHON CALLABLES

def run_ingestion(logical_date, **context):
    """Task 1: Download Olist data and upload to S3."""
    import sys
    sys.path.insert(0, "/usr/app")
    from ingestion.ingest_olist_to_s3 import ingest

    run_date = logical_date.date() if hasattr(logical_date, "date") else logical_date
    logger.info(f"Starting ingestion for date: {run_date}")
    result = ingest(run_date)

    if result.get("status") == "skipped":
        logger.info("Ingestion skipped (already done for this date).")
    else:
        successful = sum(1 for f in result.get("files", []) if f["success"])
        total = len(result.get("files", []))
        logger.info(f"Ingestion complete: {successful}/{total} files")
        if successful < total:
            raise RuntimeError(f"Only {successful}/{total} files uploaded successfully")

    # Push to XCom so downstream tasks can reference the date
    context["ti"].xcom_push(key="run_date", value=str(run_date))
    return result


def run_redshift_load(logical_date, **context):
    """Task 2: Load S3 data into Redshift staging schema."""
    import sys
    sys.path.insert(0, "/usr/app")
    from ingestion.load_to_redshift import load_all

    run_date = logical_date.date() if hasattr(logical_date, "date") else logical_date
    logger.info(f"Loading Redshift staging for date: {run_date}")
    load_all(run_date)
    logger.info("Redshift staging load complete.")


def notify_success(**context):
    """Task 7: Log pipeline success (extend to send Slack/email alerts)."""
    dag_run = context["dag_run"]
    logger.info(
        f"   Pipeline SUCCESS\n"
        f"   DAG:     {dag_run.dag_id}\n"
        f"   Run ID:  {dag_run.run_id}\n"
        f"   Date:    {dag_run.logical_date}\n"
        f"   End:     {datetime.utcnow().isoformat()}"
    )
    # TODO: Add Slack webhook or SES notification here


def notify_failure(**context):
    """Callback on task failure — logs details for debugging."""
    ti = context["task_instance"]
    logger.error(
        f"❌ Task FAILED\n"
        f"   Task:    {ti.task_id}\n"
        f"   DAG:     {ti.dag_id}\n"
        f"   Run:     {ti.run_id}\n"
        f"   Try:     {ti.try_number}\n"
        f"   Log:     {ti.log_url}"
    )

# DAG DEFINITION

with DAG(
    dag_id="ecom_full_pipeline_dag",
    description="Olist E-Commerce: S3 ingestion → Redshift → dbt transforms",
    schedule_interval="0 2 * * *",      # 02:00 UTC = 09:00 VN time
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "olist", "dbt", "production"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    #  Task 1: Ingest 
    ingest_task = PythonOperator(
        task_id="ingest_olist_to_s3",
        python_callable=run_ingestion,
        on_failure_callback=notify_failure,
        doc_md="Downloads Olist CSVs from Kaggle and uploads to S3 raw bucket.",
    )

    #  Task 2: Load Staging 
    load_staging_task = PythonOperator(
        task_id="load_staging_to_redshift",
        python_callable=run_redshift_load,
        on_failure_callback=notify_failure,
        doc_md="COPYs raw CSVs from S3 into Redshift staging schema.",
    )

    #  Task 3: dbt staging run 
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging --target prod",
        env=DBT_ENV,
        on_failure_callback=notify_failure,
        doc_md="Runs dbt staging models (views over raw Redshift tables).",
    )

    #  Task 4: dbt staging tests 
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_CMD} test --select staging --target prod",
        env=DBT_ENV,
        on_failure_callback=notify_failure,
        doc_md="Runs dbt data quality tests on staging layer. Fails pipeline on violations.",
    )

    #  Task 5: dbt mart run 
    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=f"{DBT_CMD} run --select mart --target prod",
        env=DBT_ENV,
        on_failure_callback=notify_failure,
        doc_md="Materializes all mart tables: revenue_daily, seller_performance, cohorts, categories.",
    )

    #  Task 6: dbt mart tests 
    dbt_test_mart = BashOperator(
        task_id="dbt_test_mart",
        bash_command=f"{DBT_CMD} test --select mart --target prod",
        env=DBT_ENV,
        on_failure_callback=notify_failure,
        doc_md="Runs data quality tests on mart tables before analysts can query them.",
    )

    #  Task 7: Notify success 
    notify_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Logs pipeline success. Extend to send Slack/email alerts.",
    )

    #  Task Dependencies 
    (
        ingest_task
        >> load_staging_task
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_mart
        >> dbt_test_mart
        >> notify_task
    )
