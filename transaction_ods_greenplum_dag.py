from datetime import datetime 
from airflow import DAG 
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.ssh.operators.ssh import SSHOperator 
from config import path_scripts

# Определение аргументов DAG
default_args = {
    'owner': 'ETL_Team',
    'depends_on_past': False, #зависимость от предыдущего запуска
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Инициализация DAG
dag = DAG(
    'transaction_ods_greenplum',
    default_args=default_args,
    description='daily data download from hdfs into ods and greenplum',
    start_date=datetime(2024, 02, 11),
    schedule_interval=timedelta(days=1), #интервал, запуска дага
    catchup=False,
    tags=['ETL_Team', 'Greenplum', 'ODS'],
)

spark_from_row_to_ods = SSHOperator(
    task_id='spark_from_row_to_ods',
    ssh_conn_id = 'name_node',
    cmd_timeout = None,
    command=f'cd {path_scripts} && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 transaction_transfer_row_ods.py',
    dag = dag,
)

 greenplum_query = PostgresOperator(
     task_id='greenplum_query',
     postgres_conn_id='wave9_team_b_gp',
     sql='call transaction.insert_data_transaction()'
 )

spark_from_row_to_ods >> greenplum_query
