import base64

import orjson
import pytest

from tests.routers.v3.connector.duckdb.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "customers",
            "tableReference": {
                "catalog": "jaffle_shop",
                "schema": "main",
                "table": "customers",
            },
            "columns": [
                {"name": "customer_id", "type": "integer"},
                {"name": "first_name", "type": "varchar"},
                {"name": "last_name", "type": "varchar"},
                {"name": "first_order", "type": "date"},
                {"name": "most_recent_order", "type": "date"},
                {"name": "number_of_orders", "type": "bigint"},
                {"name": "customer_lifetime_value", "type": "double"},
            ],
            "primaryKey": "customer_id",
        },
        {
            "name": "orders",
            "tableReference": {
                "catalog": "jaffle_shop",
                "schema": "main",
                "table": "orders",
            },
            "columns": [
                {"name": "order_id", "type": "integer"},
                {"name": "customer_id", "type": "integer"},
                {"name": "order_date", "type": "date"},
                {"name": "status", "type": "varchar"},
                {"name": "amount", "type": "double"},
            ],
            "primaryKey": "order_id",
        },
    ],
    "relationships": [
        {
            "name": "CustomersOrders",
            "models": ["customers", "orders"],
            "joinType": "ONE_TO_MANY",
            "condition": '"customers".customer_id = "orders".customer_id',
        }
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_query(client, manifest_str, connection_info):
    response = await client.post(
        f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT customer_id, first_name, last_name FROM "customers" ORDER BY customer_id LIMIT 1',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["columns"] == ["customer_id", "first_name", "last_name"]
    assert len(result["data"]) == 1
    assert result["data"][0] == [1, "Michael", "P."]
    assert result["dtypes"] == {
        "customer_id": "int32",
        "first_name": "string",
        "last_name": "string",
    }


async def test_query_with_limit(client, manifest_str, connection_info):
    response = await client.post(
        f"{base_url}/query",
        params={"limit": 1},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "customers" LIMIT 5',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1


async def test_query_orders(client, manifest_str, connection_info):
    response = await client.post(
        f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT order_id, customer_id, status, amount FROM "orders" ORDER BY order_id LIMIT 1',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["columns"] == ["order_id", "customer_id", "status", "amount"]
    assert len(result["data"]) == 1
    assert result["data"][0] == [1, 1, "returned", 10.0]


async def test_dry_run(client, manifest_str, connection_info):
    response = await client.post(
        f"{base_url}/query",
        params={"dryRun": True},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "customers" LIMIT 1',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 204

    response = await client.post(
        f"{base_url}/query",
        params={"dryRun": True},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "NotFound" LIMIT 1',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 422
    assert response.text is not None
