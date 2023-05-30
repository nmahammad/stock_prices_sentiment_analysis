from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from os.path import abspath, dirname

# Add the parent directory of the current file to the Python path
current_dir = dirname(abspath(__file__))
sys.path.append(dirname(current_dir))

from lib.news_fetch import news_fetch

default_args = {
    'start_date': datetime(2023, 5, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG('news_fetch_dag', default_args=default_args, schedule_interval='@daily') as dag:
    fetch_news_task = PythonOperator(
        task_id='news_fetch_task',
        python_callable=news_fetch
    )
