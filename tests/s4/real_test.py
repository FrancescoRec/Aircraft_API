from unittest.mock import Mock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bdi_api.s4.exercise import prepare_data, s4


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI(title="Exercise Test")
    app.include_router(s4)
    return app

@pytest.fixture
def client(app: FastAPI) -> TestClient:
    app.include_router(s4)
    yield TestClient(app)

class TestExercise:

    # @patch('requests.get')
    # @patch('boto3.client')
    # def test_download_data(self, mock_boto3_client, mock_requests_get):
    #     # Arrange
    #     mock_requests_get.return_value.status_code = 200
    #     mock_requests_get.return_value.text = '<a href="000000Z.json.gz"></a><a href="000005Z.json.gz">
    #</a><a href="000010Z.json.gz"></a>'
    #     mock_s3 = Mock()
    #     mock_boto3_client.return_value = mock_s3

    #     # Act
    #     result = download_data()

    #     # Assert
    #     assert result == "OK"

    @patch('os.makedirs')
    @patch('boto3.client')
    @patch('sqlite3.connect')
    def test_prepare_data(self, mock_sqlite3_connect, mock_boto3_client, mock_os_makedirs):
        # Arrange
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': '000000Z.json.gz'}, {'Key': '000005Z.json.gz'}, {'Key': '000010Z.json.gz'}]
        }
        mock_s3.get_object.return_value = {
            'Body': Mock(read=lambda: b'{"aircraft": [], "now": "2022-01-01T00:00:00Z", "messages": []}')
        }
        mock_boto3_client.return_value = mock_s3
        mock_conn = Mock()
        mock_sqlite3_connect.return_value = mock_conn

        # Act
        result = prepare_data()

        # Assert
        assert result == "OK"


#run with pytest --cov=bdi_api --cov-report=html --cov-fail-under=75
