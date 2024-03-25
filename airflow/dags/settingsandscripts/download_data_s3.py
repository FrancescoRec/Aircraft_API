from io import BytesIO

import boto3
import requests
from bs4 import BeautifulSoup

from airflow.models import Variable

s3 = boto3.client('s3', aws_access_key_id=Variable.get("aws_access_key_id"),
                   aws_secret_access_key=Variable.get("aws_secret_access_key"),
                   aws_session_token= Variable.get("aws_session_token"))

s3_bucket = Variable.get("name_bucket")

BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

def download_data(year,month):
    """Download files from URL and upload to S3 bucket"""
    response = requests.get(f"https://samples.adsbexchange.com/readsb-hist/{year}/{month}/01/")
    s3_prefix_path = f"s8/{year}{month}01/"

    if response.status_code != 200:
        return "Failed to retrieve data"

    soup = BeautifulSoup(response.text, 'html.parser')

    links = [a['href'] for a in soup.find_all('a') if a['href'].endswith('.json.gz')]

    for link in links[:1]:
        file_url = f"https://samples.adsbexchange.com/readsb-hist/{year}/{month}/01/" + link
        file_response = requests.get(file_url)
        if file_response.status_code == 200:
            file_name = link.split('/')[-1]
            file = BytesIO(file_response.content)
            file.seek(0)
            s3.upload_fileobj(file, s3_bucket, s3_prefix_path + file_name)
        else:
            print(f"Failed to download {file_url}")

    return "OK"
