"""Daily ingestion DAG: fetch every REE indicator for the previous day.

Airflow concepts on display here:
- @dag / @task (TaskFlow API): plain Python functions become tasks.
- Dynamic task mapping (.expand): one task instance per indicator, in parallel.
- data_interval_start: the day this run covers, which makes backfills work
  (`airflow dags backfill`) because the task ingests the interval's date,
  never "today".
- Idempotency lives in the client: re-running a day replaces that day's rows.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from ingestion.ree_client import INDICATORS, ingest


@dag(
    dag_id="ree_daily_ingestion",
    schedule="0 6 * * *",  # 06:00 Europe/Madrid, once yesterday's data is complete
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Madrid"),
    catchup=False,
    tags=["ree", "ingestion", "raw"],
)
def ree_daily_ingestion():
    @task
    def ingest_indicator(indicator: str, data_interval_start=None) -> int:
        return ingest(indicator, data_interval_start.date())

    ingest_indicator.expand(indicator=list(INDICATORS))


ree_daily_ingestion()
