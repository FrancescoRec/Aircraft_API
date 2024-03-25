import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bdi_api.s7.exercise import s7


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI(title="Exercise Test")
    app.include_router(s7)
    return app

@pytest.fixture
def client(app: FastAPI) -> TestClient:
    app.include_router(s7)
    yield TestClient(app)

class TestExercise:
    def test_some_endpoint2(self, client) -> None:
        with client as client:
            response = client.post("/api/s7/aircraft/prepare")
            assert response.status_code == 200
            assert response.json() == 'OK'

    def test_some_endpoint3(self, client) -> None:
        with client as client:
            response = client.get("/api/s7/aircraft/")
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
            response = client.get(f"/api/s7/aircraft/{icao}/positions")
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
            response = client.get(f"/api/s7/aircraft/{icao}/stats")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, dict)
            assert set(data.keys()) == {'had_emergency', 'max_altitude_baro', 'max_ground_speed'}
            assert isinstance(data['had_emergency'], bool)
            assert isinstance(data['max_altitude_baro'], float)
            assert isinstance(data['max_ground_speed'], float)



#poetry run with pytest --cov=bdi_api --cov-report=html --cov-fail-under=75
