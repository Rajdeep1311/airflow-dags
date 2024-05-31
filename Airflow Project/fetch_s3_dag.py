from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def fetch_data_from_s3():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_client = s3_hook.get_conn()
    obj = s3_client.get_object(Bucket='your-bucket-name', Key='data3.csv')
    df = pd.read_csv(obj['Body'])
    # Process and store df

with DAG('fetch_s3_data', default_args=default_args, schedule_interval=None) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_data_from_s3',
        python_callable=fetch_data_from_s3,
    )
