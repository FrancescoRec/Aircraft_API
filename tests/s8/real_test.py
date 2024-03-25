import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bdi_api.s8.exercise import s8


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI(title="Exercise Test")
    app.include_router(s8)
    return app

@pytest.fixture
def client(app: FastAPI) -> TestClient:
    app.include_router(s8)
    yield TestClient(app)

class TestExercise:
    def test_list_aircraft(self, client) -> None:
        with client as client:
            response = client.get("/api/s8/aircraft/")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            assert all(isinstance(x, dict) for x in data)
            for aircraft in data:
                assert set(aircraft.keys()) == {'icao', 'registration', 'type', 'owner', 'manufacturer', 'model'}


    def test_aircraft_co2(self, client) -> None:
        icao = "abfcda "
        day = "2024-02-29"
        with client as client:
            response = client.get(f"/api/s8/aircraft/{icao}/co2")
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, dict)
            assert set(data.keys()) == {'co2', 'icao'}
            assert data['icao'] == icao
            assert isinstance(data['icao'], str)
            assert isinstance(data['hours_flown'], float)
            if data['co2'] is not None:
                assert isinstance(data['co2'], float)






#poetry run pytest --cov=bdi_api --cov-report=html --cov-fail-under=75
