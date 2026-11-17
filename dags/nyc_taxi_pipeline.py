"""NYC Taxi pipeline: download → load raw → dbt build (staging + marts + tests).

Trigger from the UI ("Trigger DAG w/ config") or the CLI to pick the month:
    airflow dags trigger nyc_taxi_pipeline -c '{"year": 2024, "month": 2}'
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")


@dag(
    dag_id="nyc_taxi_pipeline",
    description="Ingest one month of NYC yellow-taxi trips and rebuild the dbt models",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "year": Param(2024, type="integer", minimum=2009),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
    tags=["nyc-taxi", "elt", "dbt"],
)
def nyc_taxi_pipeline():
    @task
    def download_trips(**context) -> str:
        from ingestion import nyc_taxi

        params = context["params"]
        return str(nyc_taxi.download_trips(int(params["year"]), int(params["month"])))

    @task
    def load_trips(path: str) -> int:
        from pathlib import Path

        from ingestion import nyc_taxi

        return nyc_taxi.load_trips(Path(path))

    @task
    def load_zones() -> int:
        from ingestion import nyc_taxi

        return nyc_taxi.load_zones()

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir . --no-use-colors",
    )

    [load_trips(download_trips()), load_zones()] >> dbt_build


nyc_taxi_pipeline()
