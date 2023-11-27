from datetime import timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


from utils.transform_module import Transform_module
transformer = Transform_module()


def read_csv():
    file_path = "/opt/airflow/data_source/Liquor_Sales.csv"
    df = transformer.read_csv(file_path)
    df.printSchema()
    print(df.show())
    print(f"Total: {df.count()}")
    transformed_df = transformer.split_lat_long(df)
    print(transformed_df.show())
    # clean_store_name = transformer.clean_store_name(df)
    # print(clean_store_name.show())

    df_county = transformed_df.select("County Number", "County").dropDuplicates()
    df = df.withColumnRenamed("County Number", "id")
    df = df.withColumnRenamed("County", "county")
    # Load
    df_county.write.mode('overwrite').csv('/opt/airflow/data_source/City.csv', header=True)

    
def transform_data(df):
    transformed_df = transformer.split_lat_long(df,"Store Location")
    print(transformed_df.show())
    print(f"Total: {df.count()}")
    return transformed_df

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
    task = PythonOperator(
        task_id='ETL',
        python_callable=read_csv,
        provide_context=True,
    )

    # transform_data_task = PythonOperator(
    #     task_id='transform_data',
    #     python_callable=transform_data,
    #     provide_context=True,
    # )

    task

    # read_csv_result = read_csv_task.output
    # transform_data_task.input = read_csv_result