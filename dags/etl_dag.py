from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extraction import extract_data
from transformation import transform_data
from loading import load_data

#define dag
dag = DAG(
    'ziko_etl',
    schedule_interval='@daily',
    start_date = datetime(2025, 2, 22),
    catchup = False,
    default_args={
        'owner' : 'airflow',
        'depends_on_past' : False,
        'retries' : 1,
        'retries_delay' : timedelta(minutes=1)
    }
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable = extract_data,
    provide_context =True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable = transform_data,
    provide_context =True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable = load_data,
    provide_context =True,
    op_args=['df', 'container_client','blob_name'],
    dag=dag
)

extract_task >> transform_task >> load_task