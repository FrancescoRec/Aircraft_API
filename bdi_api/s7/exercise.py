import json

import boto3
import numpy as np
import pandas as pd
import psycopg2
from fastapi import APIRouter, status

# from sqlalchemy import create_engine
from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()

s3 = boto3.client('s3')

name_bucket = settings.s3_bucket

user = db_credentials.username
password = db_credentials.password
host = db_credentials.host
port = db_credentials.port
database = db_credentials.database

# engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)



@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """

    response = s3.list_objects_v2(Bucket=name_bucket)

    if 'Contents' in response:
        all_files = [obj['Key'] for obj in response['Contents']]
    else:
        all_files = []


    for file in all_files:
        if file.endswith('.json.gz'):
            # Download the file from S3
            obj = s3.get_object(Bucket=name_bucket, Key=file)
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
            aircraft_data = aircraft_data.map(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
    # # Insert the data into the RDS

    # aircraft_data.to_sql('aircraft_data', engine, index=False,
    # if_exists='append')


    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
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


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
    cursor = conn.cursor()
    cursor.execute(
        f"""SELECT icao, registration, type
        FROM aircraft_data ORDER BY icao ASC LIMIT {num_results} OFFSET {num_results * page}""",
    )
    results1 = cursor.fetchall()
    cursor.close()
    return [{"icao": row[0], "registration": row[1], "type": row[2]} for row in results1]
    # return [{"icao": "0d8300", "registration": "YV3382", "type": "LJ31"}]


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO
    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
    cursor = conn.cursor()
    cursor.execute(
        f"""SELECT timestamp, lat, lon FROM aircraft_data
          WHERE icao = %s ORDER BY timestamp ASC LIMIT {num_results} OFFSET {num_results * page}""",
        (icao,)
    )
    results2 = cursor.fetchall()
    cursor.close()
    return [{"timestamp": row[0].timestamp(), "lat": row[1], "lon": row[2]} for row in results2]

    # return [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency

    FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # TODO

    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
    cursor = conn.cursor()
    cursor.execute(
        """SELECT MAX(altitude_baro) AS max_altitude_baro, MAX(ground_speed) AS max_ground_speed, bool_or(had_emergency)
        FROM aircraft_data WHERE icao = %s""",
        (icao,)
    )
    results3 = cursor.fetchall()
    cursor.close()
    return {"max_altitude_baro": results3[0][0], "max_ground_speed": results3[0][1], "had_emergency": results3[0][2]}


    # return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}
