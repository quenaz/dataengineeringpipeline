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

# Task to run the producer container
producer_task = DockerOperator(
    task_id='run_producer',
    image='producer-app:latest',
    auto_remove=True,
    dag=dag,
    command='',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge'
)

# Task to run the consumer container with arguments
consumer_task = DockerOperator(
    task_id='run_consumer',
    image='consumer-app:latest',
    auto_remove=True,
    dag=dag,
    command='--start-date 2013-06-01 --end-date 2015-06-01 --longitude-min -74.0 --longitude-max -73.0 --latitude-min 40.5 --latitude-max 41.0',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge'
)

# Task to run the Batch Processing Application (Spark) application
spark_processing = BashOperator(
    task_id='spark_processing',
    bash_command='spark-submit --class BatchProcessingApp --master local[*] ../spark/target/scala-2.13/datatransformation_2.13-1.0.jar',
    dag=dag,
)

# Set up dependencies
producer_task >> consumer_task >> spark_processing