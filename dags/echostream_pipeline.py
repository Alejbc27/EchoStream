"""
EchoStream Pipeline DAG — Raw → Bronze transformation.

ARCHITECTURE (Hybrid):
───────────────────────────────────────────────────────────────────────
  CLOUD (runs 24/7):
    Cloud Scheduler → Cloud Run Job → GCS Raw (NDJSON accumulates)

  LOCAL (this DAG, runs when your Mac + Docker are on):
    Airflow DAG → BronzeLoader → GCS Bronze (Parquet)
                → (future) dbt → Silver/Gold

WHY IS THIS BETTER THAN CALLING BronzeLoader FROM main.py?
───────────────────────────────────────────────────────────────────────
  1. Independence: if Bronze fails, extraction doesn't re-run
  2. Visibility: Airflow UI shows you what ran, when, and if it failed
  3. Retries: each task retries independently with exponential backoff
  4. Backfill: you can re-process historical dates from the UI
  5. Extensibility: adding dbt as a downstream task is one line of code

HOW TO ADD dbt LATER (Phase 2):
───────────────────────────────────────────────────────────────────────
    dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command="dbt run --models silver",
    )
    [bronze_recent, bronze_top] >> dbt_silver
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def process_recent_tracks(**kwargs):
    """
    Convert all Raw NDJSON files for recently-played tracks → Bronze Parquet.

    WHY import inside the function?
    Airflow parses ALL DAG files every 30s to discover DAGs. If imports are
    at the top and one fails, ALL DAGs stop loading. Importing here means
    the import only happens when the task actually runs.
    """
    from echostream.bronze.loader import build_bronze_loader_from_env

    loader = build_bronze_loader_from_env()
    results = loader.process_all_recent()

    total_in = sum(r["records_in"] for r in results)
    total_out = sum(r["records_out"] for r in results)
    files = len(results)

    print(f"✅ Recent tracks: {files} files, {total_in} → {total_out} records")
    for r in results:
        if r["bronze_path"]:
            print(f"   → {r['bronze_path']} ({r['records_out']} records)")

    return {"files_processed": files, "records_in": total_in, "records_out": total_out}


def process_top_tracks(**kwargs):
    """
    Convert all Raw NDJSON for top tracks (all time ranges) → Bronze Parquet.
    """
    from echostream.bronze.loader import build_bronze_loader_from_env

    loader = build_bronze_loader_from_env()
    results = loader.process_all_top()

    total_in = sum(r["records_in"] for r in results)
    total_out = sum(r["records_out"] for r in results)
    files = len(results)

    print(f"✅ Top tracks: {files} files, {total_in} → {total_out} records")
    for r in results:
        if r["bronze_path"]:
            print(f"   → {r['bronze_path']} ({r['records_out']} records)")

    return {"files_processed": files, "records_in": total_in, "records_out": total_out}


default_args = {
    "owner": "echostream",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="echostream_raw_to_bronze",
    description="Converts Raw NDJSON from GCS to typed Parquet in Bronze layer",
    default_args=default_args,
    # Run daily at 08:00 UTC (10:00 Madrid).
    # Cloud Run extracts every 2h → NDJSON accumulates in Raw.
    # BronzeLoader.process_all_*() processes ALL files in one go,
    # so daily is enough to catch up. Change to "0 */4 * * *" if you want more.
    schedule="0 8 * * *",
    start_date=datetime(2026, 3, 1),
    # Don't run for every missed day since start_date
    catchup=False,
    tags=["echostream", "bronze", "gcs"],
) as dag:
    bronze_recent = PythonOperator(
        task_id="raw_to_bronze_recent",
        python_callable=process_recent_tracks,
    )

    bronze_top = PythonOperator(
        task_id="raw_to_bronze_top",
        python_callable=process_top_tracks,
    )

    # Both tasks are independent — they run in parallel.
    # When dbt arrives, add:  [bronze_recent, bronze_top] >> dbt_task
