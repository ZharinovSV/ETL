from airflow import DAG 
from airflow.providers.ssh.operators.ssh import SSHOperator 
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from config import path_scripts

# Определение аргументов DAG
default_args = {
    "owner": "ETL_Team",
    "start_date": days_ago(1)
}

# Инициализация DAG
dag = DAG(
    'transaction_from_kafka_to_raw',
    default_args=default_args,
    description='launching consumers to transport data from kafka to raw',
    catchup=False,
    schedule_interval=timedelta(minutes=10),
    tags=['ETL_Team', 'Kafka', 'Spark', 'RAW'],
)

transaction_from_kafka_to_raw = SSHOperator(
    task_id='transaction_from_kafka_to_raw',
    ssh_conn_id = 'name_node',
    cmd_timeout = None,
    command=f'cd {path_scripts} && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 consumer_transaction_elt_raw.py',
    dag = dag,
)

activity_from_kafka_to_raw = SSHOperator(
    task_id='activity_from_kafka_to_raw',
    ssh_conn_id = 'name_node',
    cmd_timeout = None,
    command=f'cd {path_scripts} && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 consumer_activity_elt_raw.py',
    dag = dag,
)

login_from_kafka_to_raw = SSHOperator(
    task_id='login_from_kafka_to_raw',
    ssh_conn_id = 'name_node',
    cmd_timeout = None,
    command=f'cd {path_scripts} && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 consumer_login_elt_raw.py',
    dag = dag,
)

payment_from_kafka_to_raw = SSHOperator(
    task_id='payment_from_kafka_to_raw',
    ssh_conn_id = 'name_node',
    cmd_timeout = None,
    command=f'cd {path_scripts} && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 consumer_payment_elt_raw.py',
    dag = dag,
)

[transaction_from_kafka_to_raw, activity_from_kafka_to_raw, login_from_kafka_to_raw, payment_from_kafka_to_raw]
