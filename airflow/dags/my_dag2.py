from datetime import datetime

import psycopg2
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

host: str = Variable.get("host")
port: int = Variable.get("port")
username: str = Variable.get("username")
password: str = Variable.get("password")
database: str = Variable.get("database")


def flight_co2_analysis_download():
    """Download the aircraft type fuel consumption rates and insert them into Postgres database"""
    url = 'https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
    else:
        raise Exception("Failed to fetch data from the URL.")

    conn = psycopg2.connect(user=username, password=password, host=host, port=port, database=database)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS aircraft_type_fuel_consumption_rates (
            type VARCHAR(10),
            source TEXT,
            name VARCHAR(100),
            galph INTEGER,
            category VARCHAR(100)
        )
        """
    )
    for type_aircraft, aircraft_data in data.items():
        cursor.execute(
            """
            INSERT INTO aircraft_type_fuel_consumption_rates (type, source, name, galph, category)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (type_aircraft, aircraft_data['source'], aircraft_data['name'],
              aircraft_data['galph'], aircraft_data['category'])
        )

    conn.commit()
    cursor.close()

    return "OK"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1
}

dag = DAG('flight_co2_analysis_download',default_args=default_args, schedule_interval=None)

flight_co2_analysis_download_task = PythonOperator(
    task_id='flight_co2_analysis_download_task',
    python_callable=flight_co2_analysis_download,
    dag=dag,
)


