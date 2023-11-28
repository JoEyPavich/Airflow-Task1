from datetime import timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


from utils.helper import Helper
import os

helper = Helper()

TASK_ID_EXTRACT = 'extract'
TASK_ID_TRANSFORM = 'transform'
TASK_ID_LOAD = 'load'

DATA_SOURCE = '/opt/airflow/data_source'
FILENAME = 'Liquor_Sales'
EXTENSION = '.csv'

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
    filepath_loaded = kwargs['ti'].xcom_pull(key=XCOM_KEY_LOADED_DATA,
                                             task_ids=TASK_ID_EXTRACT)
    df = helper.read_parquet(filepath_loaded)
    df = helper.split_lat_long(df)
    print(df.show())
    print(f"Total: {df.count()}")
        # transformed_df = transformer.split_lat_long(df)
    # print(transformed_df.show())
    # clean_store_name = transformer.clean_store_name(df)
    # print(clean_store_name.show())

    # df_county = transformed_df.select("County Number", "County").dropDuplicates()
    # df = df.withColumnRenamed("County Number", "id")
    # df = df.withColumnRenamed("County", "county")
    # Load
    # df_county.write.mode('overwrite').csv('/opt/airflow/data_source/City.csv', header=True)
    # Convert DataFrame to JSON
    # df_json = df.toJSON().collect()
    
    # # Return JSON object
    # return json.dumps(df_json)

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
    )

    extractor >> transformer