from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.url == client.base_url.join("/docs")


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_config():
    response = client.get("/config")
    assert response.status_code == 200
    assert response.json() == {
        "wren_engine_endpoint": "http://localhost:8080",
        "remote_function_list_path": None,
        "diagnose": False,
    }


def test_update_diagnose():
    response = client.patch("/config", json={"diagnose": True})
    assert response.status_code == 200
    assert response.json()["diagnose"] is True
    response = client.get("/config")
    assert response.status_code == 200
    assert response.json()["diagnose"] is True
    response = client.patch("/config", json={"diagnose": False})
    assert response.status_code == 200
    assert response.json()["diagnose"] is False
    response = client.get("/config")
    assert response.status_code == 200
    assert response.json()["diagnose"] is False
