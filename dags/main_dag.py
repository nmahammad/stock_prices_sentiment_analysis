from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
from os.path import abspath, dirname

# Add the parent directory of the current file to the Python path
current_dir = dirname(abspath(__file__))
sys.path.append(dirname(current_dir))

from lib.stock_fetch import yahoo_fetch_yearly, yahoo_fetch_monthly
from lib.news_fetch import news_fetch
from lib.stock_data_processing import process_data, yearly_data_path, yearly_data_output
from lib.upload_to_es import index_data_stocks, index_data_news, processed_stock_path, processed_news_path

default_args = {
    'start_date': datetime(2023, 6, 11, 13, 0),  # Today at 13:00
    'retries': 0,  # Set the number of retries to 0 to run the tasks only once
}

with DAG('main_dag', default_args=default_args, schedule_interval='0 13 * * *') as dag:
    # Task 1: Trigger yahoo_fetch_yearly function
    task1 = PythonOperator(
        task_id='yahoo_fetch_yearly_task',
        python_callable=yahoo_fetch_yearly
    )

    # Task 2: Trigger yahoo_fetch_monthly function
    task2 = PythonOperator(
        task_id='yahoo_fetch_monthly_task',
        python_callable=yahoo_fetch_monthly
    )

    # Task 3: Trigger news_fetch function
    task3 = PythonOperator(
        task_id='news_fetch_task',
        python_callable=news_fetch
    )

    # Task 4: Trigger process_data function with yearly data
    task4 = PythonOperator(
        task_id='process_yearly_data_task',
        python_callable=process_data,
        op_args=[yearly_data_path, yearly_data_output]
    )

    # Task 5: Trigger news_data_processing.py file
    task5 = BashOperator(
        task_id='news_data_processing_task',
        bash_command=f'python {current_dir}/lib/news_data_processing.py',
        env={'PYTHONPATH': current_dir}
    )

    # Task 6: Trigger index_data_stocks function
    task6 = PythonOperator(
        task_id='index_stocks_data_task',
        python_callable=index_data_stocks,
        op_args=[processed_stock_path, 'metaverse_stocks']
    )

    # Task 7: Trigger index_data_news function
    task7 = PythonOperator(
        task_id='index_news_data_task',
        python_callable=index_data_news,
        op_args=[processed_news_path, 'stocks_news_correlation']
    )

    [task2, task3] >> task5
    task1 >> task4
    task4 >> task6
    task5 >> task7