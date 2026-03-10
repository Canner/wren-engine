from tests.routers.v3.connector.duckdb.conftest import base_url

v3_base_url = base_url


async def test_metadata_list_tables(client, connection_info):
    response = await client.post(
        url=f"{v3_base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    tables = response.json()
    assert len(tables) > 0

    result = next(
        filter(lambda x: x["name"] == "main.customers", tables),
        None,
    )
    assert result is not None
    assert result["primaryKey"] == ""
    assert result["properties"] == {
        "catalog": "jaffle_shop",
        "schema": "main",
        "table": "customers",
        "path": None,
    }
    assert len(result["columns"]) > 0

    customer_id_col = next(
        filter(lambda c: c["name"] == "customer_id", result["columns"]), None
    )
    assert customer_id_col is not None
    assert customer_id_col["nestedColumns"] is None
    assert customer_id_col["properties"] is None


async def test_metadata_list_constraints(client, connection_info):
    response = await client.post(
        url=f"{v3_base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.json() == []
