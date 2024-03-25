import os
import sqlite3

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status

from bdi_api.settings import Settings

settings = Settings()
url = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)

download_dir = os.path.join(settings.raw_dir, "day=20231101")
prepare_dir = os.path.join(settings.prepared_dir, "day=20231101")
path_for_get = os.path.join(prepare_dir, "aircraft.db")

@s1.post("/aircraft/download")
def download_data() -> str:

    # Create the download directory using the os module:
    os.makedirs(download_dir, exist_ok=True)


    # Fetch the HTML content and return error message if failed:
    response = requests.get(url)

    # Using soup to read through HTML content:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract links to the files using list comprehension:
    links = [a['href'] for a in soup.find_all('a') if a['href'].endswith('.json.gz')]

    # Download the first 1000 files, but first try with 1:
    for link in links[:1]:
        file_url = url + link
        file_path = os.path.join(download_dir, link)

        # Download the file using requests
        file_response = requests.get(file_url)
        if file_response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(file_response.content)

    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:

    # Create the download directory using the os module:
    os.makedirs(prepare_dir, exist_ok=True)


    all_files=os.listdir(download_dir)
    all_files = sorted(all_files)

    for file in all_files:
        if file.endswith('.json.gz'):
            complete_path = os.path.join(download_dir, file)
            with open(complete_path) as json_file:
                data_dict = pd.read_json(json_file)
                aircraft_data = pd.json_normalize(data_dict['aircraft'])
                aircraft_data['now'] = data_dict['now']
                aircraft_data['messages'] = data_dict['messages']

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
                conn = sqlite3.connect(path_for_get)

                aircraft_data.to_sql('aircraft_data', conn, index=False,
                if_exists='append',method='multi',chunksize=100)

                conn.commit()
                conn.close()

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    # Connect to the SQLite database
    conn = sqlite3.connect(path_for_get)
    cursor = conn.cursor()

    # Execute a query to fetch aircraft data
    cursor.execute('''
        SELECT DISTINCT icao, registration,type
        FROM aircraft_data
        ORDER BY icao ASC
        LIMIT ? OFFSET ?
    ''', (num_results, num_results * page))

    # Fetch the results
    results = cursor.fetchall()

    # Close the database connection
    conn.close()

    # Convert the results to a list of dictionaries
    aircraft_list = [{"icao": row[0], "registration": row[1], "type": row[2]} for row in results]

    return aircraft_list



@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:

    # Connect to the SQLite database
    conn = sqlite3.connect(path_for_get)
    cursor = conn.cursor()

    # Execute a query to fetch aircraft data
    cursor.execute('''
        SELECT timestamp, lat, lon
        FROM aircraft_data
        WHERE icao = ?
        ORDER BY timestamp ASC
        LIMIT ? OFFSET ?
    ''', (icao,num_results, num_results * page ))

    # Fetch the results
    results = cursor.fetchall()

    # Close the database connection
    conn.close()

    # Convert the results to a list of dictionaries
    aircraft_list2 = [{"timestamp": row[0], "lat": row[1], "lon": row[2]} for row in results]

    return aircraft_list2



@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> list[dict]:

    # Connect to the SQLite database
    conn = sqlite3.connect(path_for_get)
    cursor = conn.cursor()

    # Execute a query to fetch statistics for the specified aircraft
    cursor.execute('''
        SELECT MAX(altitude_baro) AS max_altitude_baro,
               MAX(ground_speed) AS max_ground_speed,
               MAX(CASE WHEN emergency = 1 THEN 1 ELSE 0 END) AS max_had_emergency
        FROM aircraft_data
        WHERE icao = ?
    ''', (icao,))

   # Fetch the results
    result = cursor.fetchone()

    # Close the database connection
    conn.close()

    # Convert the results to a list of dictionaries
    aircraft_stats = {
        "max_altitude_baro": result[0],
        "max_ground_speed": result[1],
        "max_had_emergency": result[2]
    }

    return [aircraft_stats]

