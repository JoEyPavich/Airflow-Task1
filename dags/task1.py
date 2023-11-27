from datetime import timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


from utils.transform_module import Transform_module

def read_csv():
    file_path = "/opt/airflow/data_source/Finance_data.csv"
    transformer = Transform_module()
    df = transformer.read_csv(file_path)
    print(df.show())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ETL_Liqour',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    read_csv = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv
    )

    read_csv