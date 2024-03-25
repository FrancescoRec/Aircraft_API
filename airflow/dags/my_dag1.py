from datetime import datetime

from settingsandscripts.download_data_s3 import download_data
from settingsandscripts.prepare_data_postgres import prepare_data

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1
}

def get_current_month():
    today = datetime.now()
    return today.strftime("%Y-%m")

dag = DAG('download_and_prepare',
          default_args=default_args,
          schedule_interval="@monthly")

current_month = get_current_month()
year, month = current_month.split("-")
month = month.zfill(2)

download_data_task = PythonOperator(
    task_id='download_data_task',
    python_callable=download_data,
    op_kwargs={'year': year, 'month': month},
    dag=dag,
)

prepare_data_task = PythonOperator(
    task_id='prepare_data_task',
    python_callable=prepare_data,
    op_kwargs={'year': year, 'month': month},
    dag=dag,
)

download_data_task >> prepare_data_task

