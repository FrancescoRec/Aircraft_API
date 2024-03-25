import json
import os
import sqlite3
from io import BytesIO

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status

from bdi_api.settings import Settings

settings = Settings()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

prepare_dir = os.path.join(settings.prepared_dir, "day=20231101")
path_for_get = os.path.join(prepare_dir, "aircraft.db")

s3 = boto3.client('s3')
s3_prefix_path = "raw/day=20231101/"
s3_bucket = settings.s3_bucket

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data() -> str:
    """ Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`"""

    # Fetch the HTML content and return error message if failed:
    response = requests.get(BASE_URL)

    # Using soup to read through HTML content:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract links to the files using list comprehension:
    links = [a['href'] for a in soup.find_all('a') if a['href'].endswith('.json.gz')]

    # Upload the files to the s3 bucket but first try with 1:
    for link in links[:1]:
        file_url = BASE_URL + link
        file_response = requests.get(file_url)
        if file_response.status_code == 200:
            file= BytesIO(file_response.content)
            file.seek(0)
            s3.upload_fileobj(file, s3_bucket, s3_prefix_path + link)

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s1 but using the s3 bucket and path from settings"""

    # Create the download directory using the os module:
    os.makedirs(prepare_dir, exist_ok=True)

    # Download the files from the s3 bucket:
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)

    # Extract file names from the response
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
            aircraft_data = aircraft_data.map(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            conn = sqlite3.connect(path_for_get)

            aircraft_data.to_sql('aircraft_data', conn, index=False,
            if_exists='append',method='multi',chunksize=100)

            conn.commit()
            conn.close()
    return "OK"
