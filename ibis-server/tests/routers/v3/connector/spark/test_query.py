import base64

import orjson
import pytest

from tests.routers.v3.connector.spark.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": {
                "schema": "default",
                "table": "orders",
            },
            "columns": [
                {"name": "orderkey", "expression": "o_orderkey", "type": "integer"},
                {"name": "custkey", "expression": "o_custkey", "type": "integer"},
                {
                    "name": "orderstatus",
                    "expression": "o_orderstatus",
                    "type": "varchar",
                },
                {"name": "totalprice", "expression": "o_totalprice", "type": "float"},
                {"name": "orderdate", "expression": "o_orderdate", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "concat(o_orderkey, '_', o_custkey)",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "cast('2024-01-01T23:59:59' as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "cast('2024-01-01T23:59:59' as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "cast(NULL as timestamp)",
                    "type": "timestamp",
                },
            ],
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_query(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders ORDER BY orderkey LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1

    assert result["data"][0][:6] == [1, 370, "O", "172799.49", "1996-01-02", "1_370"]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "string",
        "totalprice": "decimal128(8, 2)",
        "orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[us]",
        "timestamptz": "timestamp[us]",
        "test_null_time": "timestamp[us]",
    }


async def test_query_with_limit(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        params={"limit": 1},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1

    response = await client.post(
        url=f"{base_url}/query",
        params={"limit": 1},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 10",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1


async def test_query_with_invalid_manifest_str(client, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": "xxx",
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 422


async def test_query_without_manifest(client, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "manifestStr"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_without_sql(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={"connectionInfo": connection_info, "manifestStr": manifest_str},
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "sql"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_with_dry_run(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 204


async def test_query_with_dry_run_and_invalid_sql(
    client, manifest_str, connection_info
):
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM X",
        },
    )
    assert response.status_code == 422
    assert response.text is not None
