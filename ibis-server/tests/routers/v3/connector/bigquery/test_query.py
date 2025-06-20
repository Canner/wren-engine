import base64
import time

import orjson
import pytest

from tests.routers.v3.connector.bigquery.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": {
                "schema": "tpch_tiny",
                "table": "orders",
            },
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
                {"name": "o_custkey", "type": "integer"},
                {
                    "name": "o_orderstatus",
                    "type": "varchar",
                },
                {
                    "name": "o_totalprice",
                    "type": "double",
                },
                {"name": "o_orderdate", "type": "date"},
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
                    "expression": "cast('2024-01-01T23:59:59' as timestamp with time zone)",
                    "type": "timestamptz",
                },
                {
                    "name": "dst_utc_minus_5",
                    "expression": "cast('2024-01-15 23:00:00 America/New_York' as timestamp with time zone)",
                    "type": "timestamptz",
                },
                {
                    "name": "dst_utc_minus_4",
                    "expression": "cast('2024-07-15 23:00:00 America/New_York' as timestamp with time zone)",
                    "type": "timestamptz",
                },
            ],
            "primaryKey": "o_orderkey",
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
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        36485,
        1202,
        "F",
        "356711.63",
        "1992-06-06",
        "36485_1202",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 UTC",
        "2024-01-16 04:00:00.000000 UTC",  # utc-5
        "2024-07-16 03:00:00.000000 UTC",  # utc-4
    ]
    assert result["dtypes"] == {
        "o_orderkey": "int64",
        "o_custkey": "int64",
        "o_orderstatus": "string",
        "o_totalprice": "double",
        "o_orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[us]",
        "timestamptz": "timestamp[us, tz=UTC]",
        "dst_utc_minus_5": "timestamp[us, tz=UTC]",
        "dst_utc_minus_4": "timestamp[us, tz=UTC]",
    }


async def test_query_with_cache(client, manifest_str, connection_info):
    # add random timestamp to the query to ensure cache is not hit
    now = int(time.time())
    sql = f"SELECT *, {now} FROM orders ORDER BY o_orderkey LIMIT 1"
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": sql,
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()

    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": sql,
        },
    )

    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"
    result2 = response2.json()

    assert result1["dtypes"] == result2["dtypes"]


async def test_query_with_cache_override(client, manifest_str, connection_info):
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response1.status_code == 200

    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true&overrideCache=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )

    assert response2.status_code == 200
    assert response2.headers["X-Cache-Override"] == "true"
    assert int(response2.headers["X-Cache-Override-At"]) > int(
        response2.headers["X-Cache-Create-At"]
    )


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
    assert result["detail"][0] is not None
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
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "sql"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_without_connection_info(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "connectionInfo"]
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


async def test_timestamp_func(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT timestamp_millis(1000000) as millis, \
                timestamp_micros(1000000) as micros, \
                timestamp_seconds(1000000) as seconds",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 3
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        "1970-01-01 00:16:40.000000 UTC",
        "1970-01-01 00:00:01.000000 UTC",
        "1970-01-12 13:46:40.000000 UTC",
    ]
    assert result["dtypes"] == {
        "millis": "timestamp[us, tz=UTC]",
        "micros": "timestamp[us, tz=UTC]",
        "seconds": "timestamp[us, tz=UTC]",
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT timestamp with time zone '2000-01-01 10:00:00' < current_datetime() as compare",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        True,
    ]
    assert result["dtypes"] == {
        "compare": "bool",
    }
