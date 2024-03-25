import gzip
import json
from datetime import datetime
from io import BytesIO

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


def aircraft_database_download():
    """Download the aircraft database and insert it into Postgres database"""
    url = 'http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz'
    response = requests.get(url)
    data = []
    if response.status_code == 200:
        file = BytesIO(response.content)
        with gzip.open(file, 'rb') as gz:
            for line in gz:
                data.append(json.loads(line))


    else:
        raise Exception("Failed to fetch data from the URL.")

    conn = psycopg2.connect(user=username, password=password, host=host, port=port, database=database)
    cursor = conn.cursor()


    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS aircraft_database (
            icao varchar,
            reg varchar,
            icaotype varchar,
            year varchar,
            manufacturer varchar,
            model varchar,
            ownop varchar,
            faa_pia boolean,
            faa_ladd boolean,
            short_type varchar,
            mil boolean
        )
        """
    )

    for aircraft_data in data:
        cursor.execute(
            """
            INSERT INTO aircraft_database
            (icao, reg, icaotype, year, manufacturer, model, ownop, faa_pia, faa_ladd, short_type, mil)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (aircraft_data.get('icao'), aircraft_data.get('reg'),
            aircraft_data.get('icaotype'), aircraft_data.get('year'),
            aircraft_data.get('manufacturer'), aircraft_data.get('model'),
            aircraft_data.get('ownop'), aircraft_data.get('faa_pia'),
            aircraft_data.get('faa_ladd'), aircraft_data.get('short_type'),
            aircraft_data.get('mil'))
        )

    conn.commit()
    cursor.close()

    return "OK"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1
}

dag = DAG('aircraft_database_download6',default_args=default_args, schedule_interval=None)

aircraft_database_download_task = PythonOperator(
    task_id='aircraft_database__download_task',
    python_callable=aircraft_database_download,
    dag=dag,
)


