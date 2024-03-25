import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bdi_api.s1.exercise import s1


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI(title="Exercise Test")
    app.include_router(s1)
    return app

@pytest.fixture
def client(app: FastAPI) -> TestClient:
    app.include_router(s1)
    yield TestClient(app)

class TestExercise:
    def test_some_endpoint(self, client) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download")
            assert response.status_code == 200
            assert response.json() == 'OK'

    def test_some_endpoint2(self, client) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert response.status_code == 200
            assert response.json() == 'OK'

    def test_some_endpoint3(self, client) -> None:
        with client as client:
            response = client.get("/api/s1/aircraft/")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            if data:
                assert "icao" in data[0]
                assert "registration" in data[0]
                assert "type" in data[0]

    def test_get_aircraft_position(self, client) -> None:
        icao = "a65800"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/positions")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            if data:
                assert "timestamp" in data[0]
                assert "lat" in data[0]
                assert "lon" in data[0]

    def test_get_aircraft_statistics(self, client) -> None:
        icao = "76cdb7"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            if data:
                assert "max_altitude_baro" in data[0]
                assert "max_ground_speed" in data[0]
                assert "max_had_emergency" in data[0]



#poetry run with pytest --cov=bdi_api --cov-report=html --cov-fail-under=75
