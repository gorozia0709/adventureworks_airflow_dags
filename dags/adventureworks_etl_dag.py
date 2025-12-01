from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ==============
# CONFIG
# ==============
PROJECT_ID = "vocal-sight-476612-r8"

REGION = "europe-west1"                     # <<< CHANGE IF NEEDED
CLUSTER_NAME = "cluster-5a89"               # <<< CHANGE IF NEEDED

PYSPARK_URI_BASE = "gs://adventureworks-data-bc/pyspark-jobs/jobs"

# PostgreSQL JDBC driver JAR (now correctly loaded from DAG)
POSTGRES_DRIVER_JAR = "gs://adventureworks-data-bc/drivers/postgresql-42.7.7.jar"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="adventureworks_etl",
    default_args=default_args,
    description="AdventureWorks ETL: Cloud SQL -> GCS raw -> BigQuery dims/facts",
    schedule_interval=None,                   # run manually for now
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["adventureworks", "dataproc", "etl"],
) as dag:

    # =========================================================================
    # 1) Extract raw data from Cloud SQL PostgreSQL -> GCS parquet
    # =========================================================================
    extract_raw_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{PYSPARK_URI_BASE}/extract_raw_data.py",
            "jar_file_uris": [POSTGRES_DRIVER_JAR],    # FIXED DRIVER LOADING
            "args": [],
        },
    }

    extract_raw = DataprocSubmitJobOperator(
        task_id="extract_raw_data",
        job=extract_raw_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # =========================================================================
    # 2) Transform & Load Dimension Tables -> BigQuery
    # =========================================================================
    load_dim_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{PYSPARK_URI_BASE}/load_dim.py",
            "args": [],         # no args needed
        },
    }

    load_dim = DataprocSubmitJobOperator(
        task_id="load_dimensions",
        job=load_dim_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # =========================================================================
    # 3) Load Fact Tables (Detailed + Aggregated) -> BigQuery
    # =========================================================================
    load_fact_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{PYSPARK_URI_BASE}/final_load_fact.py",
            "args": [],
        },
    }

    load_fact = DataprocSubmitJobOperator(
        task_id="load_facts",
        job=load_fact_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # =========================================================================
    # DAG DEPENDENCIES (Pipeline Order)
    # =========================================================================
    extract_raw >> load_dim >> load_fact
