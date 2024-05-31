import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Replace with your actual API key
API_KEY = 'your-api-key'

# Function to fetch data from the OpenWeatherMap API
def fetch_weather_data():
    url = f"http://api.openweathermap.org/data/2.5/weather?q=Kolkata&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    # Process the data as needed
    print(data)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('fetch_weather_data', default_args=default_args, schedule_interval=None) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )
