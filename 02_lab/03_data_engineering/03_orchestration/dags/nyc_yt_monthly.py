from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="nyc_yellow_taxi_pipeline",
    description="Monthly ingestion of NYC TLC Yellow Taxi data",

    start_date=pendulum.datetime(
        2023,
        1,
        1,
        tz="UTC",
    ),

    # One logical run per month
    schedule="@monthly",

    # Create historical runs starting from January 2023
    catchup=True,

    # Process one month at a time
    max_active_runs=1,

    default_args={
        "owner": "lakehouse",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },

    tags=[
        "nyc-tlc",
        "yellow-taxi",
        "lakehouse",
    ],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",

        bash_command="""
        python /opt/airflow/scripts/01_ingest_bronze.py \
            --year {{ logical_date.year }} \
            --month {{ logical_date.month }}
        """,
    )