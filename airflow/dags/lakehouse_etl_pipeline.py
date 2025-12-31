from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'hpc-airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_PATH = '/opt/airflow/project'
SPARK_JOBS = f'{PROJECT_PATH}/spark_jobs'

SPARK_SUBMIT = f'''
cd {PROJECT_PATH} && \
spark-submit \
    --master local[2] \
    --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.driver.memory=2g" \
    --conf "spark.executor.memory=2g" \
'''

with DAG(
    'flights_lakehouse_etl_v2',
    default_args=default_args,
    description='Complete Bronze -> Silver -> Gold ETL pipeline with Star Schema',
    schedule='0 6 1 * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['lakehouse', 'flights', 'etl']
) as dag:

    # bronze_ingest = BashOperator(
    #     task_id='bronze_ingest',
    #     bash_command=f'{SPARK_SUBMIT} {SPARK_JOBS}/bronze_ingest/download_and_ingest.py',       
    # )

    # silver_transform = BashOperator(
    #     task_id='silver_transform',
    #     bash_command=f'{SPARK_SUBMIT} {SPARK_JOBS}/silver_transform/flights_silver_transform.py',
    # )

    # build_dimensions = BashOperator(
    #     task_id='build_dimensions',
    #     bash_command=f'{SPARK_SUBMIT} {SPARK_JOBS}/gold_marts/star_schema/build_all_dimensions.py',
    # )

    fact_flights = BashOperator(
        task_id='fact_flights',
        bash_command=f'{SPARK_SUBMIT} {SPARK_JOBS}/gold_marts/star_schema/build_fact_flights.py',
    )

    build_aggregates = BashOperator(
        task_id='build_aggregates',
        bash_command=f'{SPARK_SUBMIT} {SPARK_JOBS}/gold_marts/aggregates/build_all_aggregates.py',
    )

    # bronze_ingest >> 
    # silver_transform >> build_dimensions >> 
    fact_flights >> build_aggregates