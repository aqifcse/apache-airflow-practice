from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sqlite3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def process_data(**context):
    # Simulate data processing
    data = {"processed": "sample_data"}
    # Store in XCom for next task
    return data


def store_in_db(**context):
    ti = context['task_instance']
    processed_data = ti.xcom_pull(task_ids='process_data')
    # Create temporary table and store data
    conn = sqlite3.connect('/usr/local/airflow/airflow.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS temp_data
                 (id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute("INSERT INTO temp_data (data) VALUES (?)", (
        str(processed_data),
    ))
    conn.commit()
    conn.close()


dag = DAG(
    'complex_pipeline',
    default_args=default_args,
    description='A complex pipeline with DB storage',
    schedule_interval=timedelta(days=1),
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store_in_db',
    python_callable=store_in_db,
    provide_context=True,
    dag=dag,
)

cleanup_task = SqliteOperator(
    task_id='cleanup_temp_data',
    sqlite_conn_id='sqlite_default',
    sql=(
        'DELETE FROM temp_data WHERE date(created_at) < date("now", "-7 days")'
    ),
    dag=dag,
)

process_task >> store_task >> cleanup_task
