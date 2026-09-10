"""DAG orchestrant le pipeline complet :

  Open-Meteo (API) --extract--> JSON brut local
                     --load----> HDFS /data-lake/raw/weather/dt=...      (Data Lake)
                     --spark---> HDFS /data-lake/processed/weather/dt=... (nettoye, Parquet)
                     --load----> PostgreSQL staging_weather               (Data Warehouse)
                     --sql-----> vues propres (clean_weather, daily_weather_summary)

Le job Spark est soumis en `--deploy-mode client` : le driver tourne dans ce
conteneur Airflow (qui embarque Spark + Python), les executeurs tournent
comme conteneurs YARN sur le nodemanager. Comme la transformation n'utilise
aucune UDF Python (uniquement des expressions Spark SQL natives), aucun
interpreteur Python n'est requis cote executeur : seul le driver en a
besoin, ce qui evite d'avoir a installer Spark/Python dans les images Hadoop
(bde2020) qui composent le cluster.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

RAW_LOCAL_DIR = "/tmp/raw_weather"
HDFS_RAW_DIR_TPL = "/data-lake/raw/weather/dt={ds}"
HDFS_PROCESSED_DIR_TPL = "/data-lake/processed/weather/dt={ds}"

PIPELINE_DIR = "/opt/pipeline"


def _extract(**context):
    from ingestion.extract_openmeteo import run

    cities = os.environ["WEATHER_CITIES"]
    path = run(cities, RAW_LOCAL_DIR)
    context["ti"].xcom_push(key="raw_local_path", value=path)


def _load_raw_to_hdfs(**context):
    from etl.hdfs_utils import upload_file

    local_path = context["ti"].xcom_pull(key="raw_local_path", task_ids="extract_openmeteo")
    hdfs_dir = HDFS_RAW_DIR_TPL.format(ds=context["ds"])
    upload_file(local_path, hdfs_dir)


def _load_to_postgres(**context):
    from etl.load_to_postgres import load

    hdfs_dir = HDFS_PROCESSED_DIR_TPL.format(ds=context["ds"])
    n = load(hdfs_dir)
    print(f"Loaded {n} rows for {context['ds']}")


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_pipeline_dag",
    description="API Open-Meteo -> Data Lake (HDFS) -> Spark -> PostgreSQL DWH",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["hadoop", "spark", "weather"],
) as dag:

    extract_openmeteo = PythonOperator(
        task_id="extract_openmeteo",
        python_callable=_extract,
    )

    load_raw_to_hdfs = PythonOperator(
        task_id="load_raw_to_hdfs",
        python_callable=_load_raw_to_hdfs,
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "spark-submit "
            "--master yarn "
            "--deploy-mode client "
            "--driver-memory 512m "
            "--executor-memory 512m "
            "--executor-cores 1 "
            "--num-executors 1 "
            f"{PIPELINE_DIR}/spark_jobs/transform_weather.py "
            f"--input-dir hdfs://namenode:9000{HDFS_RAW_DIR_TPL.format(ds='{{ ds }}')} "
            f"--output-dir hdfs://namenode:9000{HDFS_PROCESSED_DIR_TPL.format(ds='{{ ds }}')}"
        ),
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=_load_to_postgres,
    )

    run_clean_sql = BashOperator(
        task_id="run_clean_sql",
        bash_command=(
            "PGPASSWORD=$DWH_POSTGRES_PASSWORD psql "
            "-h postgres-dwh -U $DWH_POSTGRES_USER -d $DWH_POSTGRES_DB "
            f"-f {PIPELINE_DIR}/sql/02_clean_views.sql"
        ),
    )

    extract_openmeteo >> load_raw_to_hdfs >> spark_transform >> load_to_postgres >> run_clean_sql
