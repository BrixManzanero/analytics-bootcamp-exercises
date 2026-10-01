from datetime import timedelta
import logging
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def ingest_iceberg(month):
    # Installed in Airflow; Spark and its Java dependencies stay in Spark's container.
    import docker
    client = docker.from_env(timeout=3600)
    try:
        container = client.containers.get("asb-spark-iceberg")
        if container.status != "running":
            raise AirflowException("asb-spark-iceberg must be running")
        command = [
            "spark-submit", "--master", "local[*]",
            "/opt/spark/scripts/02_ingest_bronze.py",
            "--month", month,
        ]
        execution = client.api.exec_create(container.id, command, stdout=True, stderr=True)
        for chunk in client.api.exec_start(execution["Id"], stream=True):
            logging.info(chunk.decode("utf-8", errors="replace").rstrip())
        result = client.api.exec_inspect(execution["Id"])
        if result["Running"] or result["ExitCode"] != 0:
            raise AirflowException(f"Spark ingestion failed: exit code {result['ExitCode']}")
    finally:
        client.close()


with DAG(
    dag_id="nyc_yellow_taxi_pipeline",
    description="Monthly Yellow Taxi ingestion: SeaweedFS raw to Iceberg bronze",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "lakehouse",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["nyc-tlc", "yellow-taxi", "lakehouse"],
) as dag:
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="""
        python -u /opt/airflow/scripts/01_ingest_raw.py \
            --year {{ data_interval_start.year }} \
            --month {{ data_interval_start.month }}
        """,
    )

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=ingest_iceberg,
        op_kwargs={"month": "{{ data_interval_start.strftime('%Y-%m') }}"},
    )

    ingest_raw >> ingest_bronze
