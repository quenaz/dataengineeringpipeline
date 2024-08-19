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
    bash_command='python ../streamer/producer.py',
    dag=dag,
)

# Task 2: Bash task for data ingestion
data_consumer = BashOperator(
    task_id='data_consumer',
    bash_command='python ../streamer/consumer.py --start-date 2013-06-01 --end-date 2015-06-01',
    dag=dag,
)

# Task 3: Spark job to process data
spark_processing = BashOperator(
    task_id='spark_processing',
    bash_command='spark-submit --class BatchProcessingApp --master local[*] ../spark/target/scala-2.13/datatransformation_2.13-1.0.jar',
    dag=dag,
)

# Set up dependencies
data_ingestion >> data_consumer >> spark_processing