import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import os
from constants import (
    BOOTSTRAP_SERVERS, TOPIC, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DB
)
sys.path.insert(0, '/opt/airflow/dags/src')


from kafka_client.kafka_producer import stream


# start_date = datetime.today() - timedelta(days=1)
start_date = datetime(2024, 1, 1)

default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "retries": 1,  # number of retries before failing the task
    "retry_delay": timedelta(seconds=5),
}


with DAG(
    dag_id="kafka_spark_dag",       
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    kafka_stream_task = PythonOperator(
        task_id="kafka_data_stream",
        python_callable=stream,
        dag=dag,
    )

    spark_stream_task = DockerOperator(
        task_id="pyspark_consumer",
        image="Mohamed/spark:latest",
        api_version="auto",
        auto_remove=True,
        command="./bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/bitnami/spark/spark_pgsql.py",
        docker_url='tcp://docker-proxy:2375',
        environment={
            'SPARK_LOCAL_HOSTNAME': 'localhost',
            'KAFKA_BROKER': BOOTSTRAP_SERVERS, 
            'KAFKA_TOPIC': TOPIC,
            'POSTGRES_URL': POSTGRES_URL,
            'POSTGRES_USER': POSTGRES_USER,
            'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
            'POSTGRES_HOST': POSTGRES_HOST,
            "POSTGRES_DB": POSTGRES_DB
        },
        network_mode="airflow-kafka",
        mount_tmp_dir=False,  # ✅ Disable tmp directory mounting
        dag=dag,
    )



    kafka_stream_task >> spark_stream_task