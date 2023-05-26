from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Now you can import 'fetch_yahoo' directly
import sys
from os.path import abspath, dirname

# Add the parent directory of the current file to the Python path
current_dir = dirname(abspath(__file__))
sys.path.append(dirname(current_dir))

# Now you can import 'fetch_yahoo' directly
from lib.yahoo_fetch import yahoo_fetch

# Now you can import 'yahoo_fetch' directly
# from ..lib.yahoo_fetch import yahoo_fetch

default_args = {
    'start_date': datetime(2023, 5, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG('yahoo_fetch_dag', default_args=default_args, schedule_interval='@daily') as dag:
    fetch_yahoo_task = PythonOperator(
        task_id='yahoo_fetch_task',
        python_callable=yahoo_fetch
    )
