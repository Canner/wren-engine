import pytest

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


async def test_root(client):
    response = await client.get("/")
    assert response.status_code == 307
    assert response.next_request.url == client.base_url.join("/docs")


async def test_health(client):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_config(client):
    response = await client.get("/config")
    assert response.status_code == 200
    assert response.json() == {
        "wren_engine_endpoint": "http://localhost:8080",
        "remote_function_list_path": None,
        "remote_white_function_list_path": None,
        "diagnose": False,
        "app_timeout_seconds": 240,
    }


async def test_update_diagnose(client):
    response = await client.patch("/config", json={"diagnose": True})
    assert response.status_code == 200
    assert response.json()["diagnose"] is True
    response = await client.get("/config")
    assert response.status_code == 200
    assert response.json()["diagnose"] is True
    response = await client.patch("/config", json={"diagnose": False})
    assert response.status_code == 200
    assert response.json()["diagnose"] is False
    response = await client.get("/config")
    assert response.status_code == 200
    assert response.json()["diagnose"] is False
