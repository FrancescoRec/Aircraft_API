import pytest
from fastapi.testclient import TestClient

import bdi_api
from bdi_api.app import app


@pytest.fixture
def client() -> TestClient:
    """We include our router for the examples"""
    return TestClient(app)

class TestApp:
    def test_health(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == "ok"
    def test_version(self, client):
            response = client.get("/version")
            assert response.status_code == 200
            data = response.json()
            assert data.get("version") == bdi_api.__version__
