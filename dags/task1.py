from datetime import timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


from utils.helper import Helper
import os
import yaml

with open('/opt/airflow/config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
helper = Helper()

TASK_ID_EXTRACT = 'extract'
TASK_ID_TRANSFORM = 'transform'
TASK_ID_LOAD = 'load'

DATA_SOURCE = '/opt/airflow/data_source'
FILENAME = config['file_name']
EXTENSION = config['extension']

XCOM_KEY_LOADED_DATA = 'filepath_loaded'
XCOM_KEY_TRANSFORMED_DATA = 'filepath_transformed'


def get_data_raw_file_path(filename,extension):
    return os.path.join(DATA_SOURCE, f"{filename}{extension}")


def get_data_loaded_path(filename):
    return os.path.join(DATA_SOURCE, f"{filename}_loaded.parquet")


def get_data_transformed_path(filename):
    return os.path.join(DATA_SOURCE, f"{filename}_transformed.parquet")

def extractor(**kwargs):
    filename = kwargs['filename']
    extension = kwargs['extension']
    df = helper.read_csv(get_data_raw_file_path(filename,extension))

    # log
    df.printSchema()
    print(df.show())
    print(f"Total: {df.count()}")

    loaded_path = get_data_loaded_path(filename)
    df.write.mode("overwrite").parquet(loaded_path)
    kwargs['ti'].xcom_push(key=XCOM_KEY_LOADED_DATA, value=loaded_path)
    return 'Extraction succesful. Parquet file stored an availabe in XCom.'
    
def transformer(**kwargs):
    # Read temp loaded
    filename = kwargs['filename']
    filepath_loaded = kwargs['ti'].xcom_pull(key=XCOM_KEY_LOADED_DATA,
                                             task_ids=TASK_ID_EXTRACT)
    df = helper.read_parquet(filepath_loaded)


    # Transform
    df = helper.split_lat_long(df)
    df = helper.rename_col(df)
    df = helper.split_store_name_and_city(df)
    print(df.show())
    for table in config['tables']:
        table_name = table['name']
        columns = table['columns']
        print(table_name)
        for column in columns:
            print(type(column))
            print(column)
    
        for column in columns:
            print(type(column))
            print(column)
    
    # Write Transformed
    filepath_tranformed = get_data_transformed_path(filename)
    df.write.mode("overwrite").parquet(filepath_tranformed)
    kwargs['ti'].xcom_push(key=XCOM_KEY_TRANSFORMED_DATA, value=filepath_tranformed)

def load(**kwargs):
    filename = kwargs['filename']
    filepath_tranformed = kwargs['ti'].xcom_pull(key=XCOM_KEY_TRANSFORMED_DATA,
                                             task_ids=TASK_ID_TRANSFORM)
    # Read temp Transformed
    df = helper.read_parquet(filepath_tranformed)
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
    extractor = PythonOperator(
        task_id=TASK_ID_EXTRACT,
        python_callable=extractor,
        op_kwargs={'filename':FILENAME,'extension':EXTENSION}
    )

    transformer = PythonOperator(
        task_id=TASK_ID_TRANSFORM,
        python_callable=transformer,
        op_kwargs={'filename':FILENAME,'extension':EXTENSION}
    )

    load = PythonOperator(
    task_id=TASK_ID_LOAD,
    python_callable=load,
    op_kwargs={'filename':FILENAME,'extension':EXTENSION}
    )

    extractor >> transformer >> load