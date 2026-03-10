import pytest

from tests.routers.v3.connector.doris.conftest import base_url

v2_base_url = "/v2/connector/doris"


async def test_metadata_list_tables(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    tables = response.json()
    assert len(tables) > 0

    # Find the orders table in wren_test schema
    result = next(
        filter(lambda x: x["name"] == "wren_test.orders", tables),
        None,
    )
    assert result is not None
    assert result["name"] == "wren_test.orders"
    assert result["properties"] is not None
    assert result["properties"]["schema"] == "wren_test"
    assert result["properties"]["table"] == "orders"
    assert len(result["columns"]) > 0

    # Check a specific column (o_orderkey)
    orderkey_col = next(
        filter(lambda col: col["name"] == "o_orderkey", result["columns"]), None
    )
    assert orderkey_col is not None
    assert orderkey_col["type"] in ["INTEGER", "INT"]
    assert orderkey_col["nestedColumns"] is None

    # Check decimal column (o_totalprice)
    price_col = next(
        filter(lambda col: col["name"] == "o_totalprice", result["columns"]), None
    )
    assert price_col is not None
    assert price_col["type"] in ["DECIMAL", "DECIMALV3"]

    # Check date column (o_orderdate)
    date_col = next(
        filter(lambda col: col["name"] == "o_orderdate", result["columns"]), None
    )
    assert date_col is not None
    assert date_col["type"] in ["DATE", "DATEV2"]


async def test_metadata_list_constraints(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    constraints = response.json()

    # Doris does not support foreign key constraints, expect empty list
    assert constraints == []


async def test_metadata_db_version(client, connection_info):
    response = await client.post(
        url=f"{v2_base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text is not None

    # Doris returns MySQL-compatible version string (e.g. "5.7.99")
    version_str = response.text
    assert version_str is not None
    assert len(version_str) > 0
