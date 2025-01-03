import base64

import orjson
import pytest

from tests.routers.v3.connector.postgres.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": {
                "schema": "public",
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
                    "type": "text",
                    "isHidden": True,
                },
                {
                    "name": "o_totalprice_double",
                    "type": "double",
                    "expression": "cast(o_totalprice as double)",
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
        {
            "name": "customer",
            "tableReference": {
                "schema": "public",
                "table": "customer",
            },
            "columns": [
                {"name": "c_custkey", "type": "integer"},
                {"name": "c_name", "type": "varchar"},
                {"name": "orders", "type": "orders", "relationship": "orders_customer"},
                {
                    "name": "sum_totalprice",
                    "type": "double",
                    "isCalculated": True,
                    "expression": "sum(orders.o_totalprice_double)",
                },
            ],
            "primaryKey": "c_custkey",
        },
    ],
    "relationships": [
        {
            "name": "orders_customer",
            "models": ["orders", "customer"],
            "joinType": "many_to_one",
            "condition": "orders.o_custkey = customer.c_custkey",
        }
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
    assert len(result["columns"]) == 10
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 UTC",
        "2024-01-16 04:00:00.000000 UTC",  # utc-5
        "2024-07-16 03:00:00.000000 UTC",  # utc-4
        "172799.49",
        "1_370",
        370,
        "1996-01-02",
        1,
        "O",
    ]
    assert result["dtypes"] == {
        "o_orderkey": "int32",
        "o_custkey": "int32",
        "o_orderstatus": "object",
        "o_totalprice_double": "float64",
        "o_orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "object",
        "timestamptz": "object",
        "dst_utc_minus_5": "object",
        "dst_utc_minus_4": "object",
    }


async def test_query_with_connection_url(client, manifest_str, connection_url):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 10
    assert len(result["data"]) == 1
    assert result["data"][0][0] == "2024-01-01 23:59:59.000000"
    assert result["dtypes"] is not None


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
    assert response.text == "Base64 decode error: Invalid padding"


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


async def test_query_to_many_calculation(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT sum_totalprice FROM wren.public.customer limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1
    assert result["dtypes"] == {"sum_totalprice": "float64"}

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT sum_totalprice FROM wren.public.customer where c_name = 'Customer#000000001' limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1
    assert result["dtypes"] == {"sum_totalprice": "float64"}

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name, sum_totalprice FROM wren.public.customer limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 2
    assert len(result["data"]) == 1
    assert result["dtypes"] == {"c_name": "object", "sum_totalprice": "float64"}

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_custkey, sum_totalprice FROM wren.public.customer limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 2
    assert len(result["data"]) == 1
    assert result["dtypes"] == {"c_custkey": "int32", "sum_totalprice": "float64"}


@pytest.mark.skip(reason="Datafusion does not implement filter yet")
async def test_query_with_keyword_filter(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT count(*) FILTER(WHERE o_orderkey != NULL) FROM wren.public.orders",
        },
    )
    assert response.status_code == 200
    assert response.text is not None
