from tests.routers.v3.connector.postgres.conftest import base_url


async def test_get_knowledge(client):
    url = f"{base_url}/knowledge"
    response = await client.get(url)
    assert response.status_code == 200
    data = response.json()
    assert "text_to_sql_rule" in data
    assert "instructions" in data
    instructions = data["instructions"]
    assert "calculated_field" in instructions
