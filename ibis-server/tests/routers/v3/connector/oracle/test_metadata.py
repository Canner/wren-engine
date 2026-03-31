from tests.routers.v3.connector.oracle.conftest import base_url


async def test_metadata_list_tables(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    result = response.json()
    result = next(filter(lambda x: x["name"] == "SYSTEM.ORDERS", result))
    assert result is not None

    assert result["primaryKey"] is not None
    assert result["description"] == "This is a table comment"
    assert result["properties"] == {
        "catalog": "",
        "schema": "SYSTEM",
        "table": "ORDERS",
        "path": None,
    }
    assert len(result["columns"]) == 9
    assert result["columns"][8] == {
        "name": "O_COMMENT",
        "nestedColumns": None,
        "type": "TEXT",
        "notNull": False,
        "description": "This is a comment",
        "properties": None,
    }


async def test_metadata_list_constraints(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    result = response.json()
    # Oracle has constraints on system tables
    assert len(result) > 0


async def test_metadata_db_version(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert "23.0" in response.text
