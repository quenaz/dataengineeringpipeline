from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'nyc_taxi_fare_processing',
    default_args=default_args,
    description='DAG for NYC Taxi Fare Processing',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Bash task for data ingestion
data_ingestion = BashOperator(
    task_id='data_ingestion',
    bash_command='python streamer/streamer.py',
    dag=dag,
)

# Task 2: Spark job to process data
spark_processing = BashOperator(
    task_id='spark_processing',
    bash_command='spark-submit --class NYC_Taxi_Fare_Processing --master local[*] ./spark/job/nyc_taxi_fare_processing.jar',
    dag=dag,
)

# Set up dependencies
data_ingestion >> spark_processing