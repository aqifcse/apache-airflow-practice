from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_data():
    # Simulate data extraction
    return {"data": "sample_extracted_data"}


def transform_data(**context):
    ti = context['task_instance']
    extracted_data = ti.xcom_pull(task_ids='extract')
    # Simulate transformation
    transformed_data = extracted_data["data"].upper()
    return {"transformed_data": transformed_data}


def load_data(**context):
    ti = context['task_instance']
    transformed_data = ti.xcom_pull(task_ids='transform')
    # Simulate loading data
    print(f"Loading data: {transformed_data}")


dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
