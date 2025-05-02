from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
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


def process_data(**context):
    # Simulate data processing
    data = {"processed": "sample_data"}
    # Store in XCom for next task
    return data


def store_in_db(**context):
    ti = context['task_instance']
    processed_data = ti.xcom_pull(task_ids='process_data')

    try:
        # Use PostgresHook to handle connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Create temporary table and store data
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS temp_data (
            id SERIAL PRIMARY KEY,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )'''

        insert_sql = "INSERT INTO temp_data (data) VALUES (%s)"

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                cur.execute(insert_sql, (str(processed_data),))
                conn.commit()
    except Exception as e:
        context['task_instance'].xcom_push(key='error', value=str(e))
        raise AirflowException(f"Database operation failed: {str(e)}")


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

cleanup_task = PostgresOperator(
    task_id='cleanup_temp_data',
    postgres_conn_id='postgres_default',
    sql=(
        'DELETE FROM temp_data WHERE'
        ' created_at < CURRENT_DATE - INTERVAL \'7 days\'',
    ),
    dag=dag,
)

process_task >> store_task >> cleanup_task
