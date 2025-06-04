import base64

import orjson
import pytest

from app.dependencies import X_WREN_FALLBACK_DISABLE
from tests.routers.v3.connector.oracle.conftest import base_url

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "dataSource": "oracle",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "schema": "SYSTEM",
                "table": "ORDERS",
            },
            "columns": [
                {"name": "orderkey", "expression": '"O_ORDERKEY"', "type": "number"},
                {"name": "custkey", "expression": '"O_CUSTKEY"', "type": "number"},
                {
                    "name": "orderstatus",
                    "expression": '"O_ORDERSTATUS"',
                    "type": "varchar",
                },
                {
                    "name": "totalprice",
                    "expression": '"O_TOTALPRICE"',
                    "type": "number",
                },
                {
                    "name": "O_ORDERDATE",
                    "type": "float64",
                    "isHidden": True,
                },
                {
                    "name": "orderdate",
                    "expression": 'TRUNC("O_ORDERDATE")',
                    "type": "date",
                },
                {
                    "name": "order_cust_key",
                    "expression": '"O_ORDERKEY" || \'_\' || "O_CUSTKEY"',
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "TO_TIMESTAMP('2024-01-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS')",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "TO_TIMESTAMP_TZ( '2024-01-01 23:59:59.000000 +00:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')",
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "CAST(NULL AS TIMESTAMP)",
                    "type": "timestamp",
                },
            ],
            "primaryKey": "orderkey",
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
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    # include one hidden column
    assert len(result["columns"]) == len(manifest["models"][0]["columns"]) - 1
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        1,
        370,
        "O",
        "172799.49",
        "1996-01-02 00:00:00.000000",
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 UTC",
        None,
    ]
    assert result["dtypes"] == {
        "orderkey": "int64",
        "custkey": "int64",
        "orderstatus": "object",
        "totalprice": "object",
        "orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "object",
        "timestamptz": "object",
        "test_null_time": "datetime64[ns]",
    }


async def test_query_with_connection_url(client, manifest_str, connection_url):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    # include one hidden column
    assert len(result["columns"]) == len(manifest["models"][0]["columns"]) - 1
    assert len(result["data"]) == 1
    assert result["data"][0][0] == 1
    assert result["dtypes"] is not None


async def test_query_with_dsn(client, manifest_str, connection_info):
    dsn = f"{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {
                "dsn": dsn,
                "user": connection_info["user"],
                "password": connection_info["password"],
            },
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    # include one hidden column
    assert len(result["columns"]) == len(manifest["models"][0]["columns"]) - 1
    assert len(result["data"]) == 1
    assert result["data"][0][0] == 1
    assert result["dtypes"] is not None
