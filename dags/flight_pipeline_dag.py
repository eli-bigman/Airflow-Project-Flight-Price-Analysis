from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

# Import modular tasks
from tasks.ingestion import extract_and_load_staging
from tasks.transformation import transform_and_load_analytics
from tasks.validation import validate_row_counts

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'flight_price_pipeline',
    default_args=default_args,
    description='ETL pipeline for Flight Price Analysis',
    schedule_interval='@daily',
    catchup=False,
)

# --- Sensors ---
wait_for_mysql = SqlSensor(
    task_id='wait_for_mysql',
    conn_id='mysql_default',
    sql="SELECT 1",
    poke_interval=10,
    timeout=600,
    dag=dag,
)

wait_for_postgres = SqlSensor(
    task_id='wait_for_postgres',
    conn_id='postgres_default',
    sql="SELECT 1",
    poke_interval=10,
    timeout=600,
    dag=dag,
)

# --- Tasks ---
t1 = PythonOperator(
    task_id='load_csv_to_mysql_staging',
    python_callable=extract_and_load_staging,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_and_load_star_schema',
    python_callable=transform_and_load_analytics,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_row_counts',
    python_callable=validate_row_counts,
    dag=dag,
)

# --- Dependencies ---
[wait_for_mysql, wait_for_postgres] >> t1 >> t2 >> validate_task
