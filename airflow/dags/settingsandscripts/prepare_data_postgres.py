
import json

import boto3
import numpy as np
import pandas as pd
import psycopg2

from airflow.models import Variable

s3 = boto3.client('s3', aws_access_key_id=Variable.get("aws_access_key_id"),
                   aws_secret_access_key=Variable.get("aws_secret_access_key"),
                   aws_session_token= Variable.get("aws_session_token"))


s3_bucket = Variable.get("name_bucket")


def prepare_data(year,month):
    """Get the raw data from s3 and insert it into postgres database"""

    host: str = Variable.get("host")
    port: int = Variable.get("port")
    username: str = Variable.get("username")
    password: str = Variable.get("password")
    database: str = Variable.get("database")

    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=f's8/{year}{month}01/')

    if 'Contents' in response:
        all_files = [obj['Key'] for obj in response['Contents']]
    else:
        all_files = []

    for file in all_files:
        if file.endswith('.json.gz'):
            # Download the file from S3
            obj = s3.get_object(Bucket=s3_bucket, Key=file)
            file_content = obj['Body'].read().decode('utf-8')

            # Process the file content using Pandas
            data_dict = json.loads(file_content)
            aircraft_data = pd.json_normalize(data_dict['aircraft'])
            aircraft_data['now'] = data_dict['now']
            aircraft_data['messages'] = data_dict['messages']

        # Process the aircraft data as needed
            aircraft_data.rename(
                    columns={
                                "hex":"icao",
                                "r":"registration",
                                "type":"messages_type",
                                "t":"type",
                                "alt_baro":"altitude_baro",
                                "gs":"ground_speed"
                                    },inplace=True)

            aircraft_data["had_emergency"]=aircraft_data["emergency"].apply(lambda x:x is not ["none",None])
            aircraft_data["timestamp"]=pd.to_datetime(aircraft_data["now"],unit='s')
            aircraft_data = aircraft_data.apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

    conn = psycopg2.connect(user=username, password=password, host=host, port=port, database=database)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS aircraft_data (
            icao varchar,
            registration varchar,
            messages_type varchar,
            type varchar,
            altitude_baro float,
            ground_speed float,
            had_emergency boolean,
            timestamp timestamp,
            lat float,
            lon float
        )
        """
    )

    for _, row in aircraft_data.iterrows():
        row = row.where(pd.notnull(row), None)
        row['altitude_baro'] = row['altitude_baro'] if np.isreal(row['altitude_baro']) else None
        row['ground_speed'] = row['ground_speed'] if np.isreal(row['ground_speed']) else None
        cursor.execute(
            """INSERT INTO aircraft_data
            (icao, registration, messages_type, type, altitude_baro, ground_speed, had_emergency, timestamp, lat, lon)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (row['icao'], row['registration'], row['messages_type'], row['type'], row['altitude_baro'],
              row['ground_speed'], row['had_emergency'], row['timestamp'], row['lat'], row['lon'])
        )

    conn.commit()
    cursor.close()


    return "OK"
