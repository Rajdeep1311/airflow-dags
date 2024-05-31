from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('master_dag', default_args=default_args, schedule_interval='@daily') as dag:
    start = DummyOperator(task_id='start')

    trigger_s3_dag = TriggerDagRunOperator(
        task_id='trigger_fetch_s3_data',
        trigger_dag_id='fetch_s3_data',
        wait_for_completion=True,
    )

    trigger_mysql_dag = TriggerDagRunOperator(
        task_id='trigger_fetch_mysql_data',
        trigger_dag_id='fetch_mysql_data',
        wait_for_completion=True,
    )

    trigger_weather_dag = TriggerDagRunOperator(
        task_id='trigger_fetch_weather_data',
        trigger_dag_id='fetch_weather_data',
        wait_for_completion=True,
    )

    end = DummyOperator(task_id='end')

    start >> trigger_s3_dag >> trigger_mysql_dag >> trigger_weather_dag >> end