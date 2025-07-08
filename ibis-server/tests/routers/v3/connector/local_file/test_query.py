import base64

import orjson
import pytest

from tests.routers.v3.connector.local_file.conftest import base_url

manifest = {
    "catalog": "my_calalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "table": "tests/resource/tpch/data/orders.parquet",
            },
            "columns": [
                {"name": "orderkey", "expression": "o_orderkey", "type": "integer"},
                {"name": "custkey", "expression": "o_custkey", "type": "integer"},
                {
                    "name": "orderstatus",
                    "expression": "o_orderstatus",
                    "type": "varchar",
                },
                {
                    "name": "totalprice",
                    "expression": "o_totalprice",
                    "type": "float",
                },
                {"name": "orderdate", "expression": "o_orderdate", "type": "date"},
            ],
            "primaryKey": "orderkey",
        },
        {
            "name": "Customer",
            "tableReference": {
                "table": "tests/resource/tpch/data/customer.parquet",
            },
            "columns": [
                {
                    "name": "custkey",
                    "type": "integer",
                    "expression": "c_custkey",
                },
                {
                    "name": "Orders",
                    "type": "Orders",
                    "relationship": "CustomerOrders",
                },
                {
                    "name": "sum_totalprice",
                    "type": "float",
                    "isCalculated": True,
                    "expression": 'sum("Orders".totalprice)',
                },
            ],
            "primaryKey": "custkey",
        },
    ],
    "relationships": [
        {
            "name": "CustomerOrders",
            "models": ["Customer", "Orders"],
            "joinType": "ONE_TO_MANY",
            "condition": '"Customer".custkey = "Orders".custkey',
        }
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_query(client, manifest_str):
    response = await client.post(
        f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT orderkey, custkey, orderstatus, totalprice, orderdate FROM "Orders" LIMIT 1',
            "connectionInfo": {
                "url": "tests/resource/tpch",
                "format": "parquet",
            },
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        1,
        370,
        "O",
        "172799.49",
        "1996-01-02",
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "string",
        "totalprice": "decimal128(15, 2)",
        "orderdate": "date32[day]",
    }


async def test_query_with_limit(client, manifest_str):
    response = await client.post(
        f"{base_url}/query",
        params={"limit": 1},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" limit 2',
            "connectionInfo": {
                "url": "tests/resource/tpch",
                "format": "parquet",
            },
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1


async def test_query_calculated_field(client, manifest_str):
    response = await client.post(
        f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT custkey, sum_totalprice FROM "Customer" WHERE custkey = 370',
            "connectionInfo": {
                "url": "tests/resource/tpch",
                "format": "parquet",
            },
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 2
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        370,
        2860895.79,
    ]
    assert result["dtypes"] == {
        "custkey": "int32",
        "sum_totalprice": "double",
    }


async def test_dry_run(client, manifest_str):
    response = await client.post(
        f"{base_url}/query",
        params={"dryRun": True},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
            "connectionInfo": {
                "url": "tests/resource/tpch",
                "format": "parquet",
            },
        },
    )
    assert response.status_code == 204

    response = await client.post(
        f"{base_url}/query",
        params={"dryRun": True},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "NotFound" LIMIT 1',
            "connectionInfo": {
                "url": "tests/resource/tpch",
                "format": "parquet",
            },
        },
    )
    assert response.status_code == 422
    assert response.text is not None


async def test_query_duckdb_format(client):
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
                    {"name": "customer_lifetime_value", "type": "double"},
                    {"name": "first_name", "type": "varchar"},
                    {"name": "first_order", "type": "date"},
                    {"name": "last_name", "type": "varchar"},
                    {"name": "most_recent_order", "type": "date"},
                    {"name": "number_of_orders", "type": "integer"},
                ],
            },
        ],
        "relationships": [],
        "views": [],
    }
    response = await client.post(
        f"{base_url}/query",
        json={
            "manifestStr": base64.b64encode(orjson.dumps(manifest)).decode("utf-8"),
            "sql": "SELECT * FROM customers LIMIT 1",
            "connectionInfo": {
                "url": "tests/resource/test_file_source",
                "format": "duckdb",
            },
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
