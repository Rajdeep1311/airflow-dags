from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def fetch_data_from_mysql():
    mysql_hook = MySqlHook(mysql_conn_id='my-sql-default')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM data1")
    result = cursor.fetchall()
    # Process and store result

with DAG('fetch_mysql_data', default_args=default_args, schedule_interval=None) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_data_from_mysql',
        python_callable=fetch_data_from_mysql,
    )