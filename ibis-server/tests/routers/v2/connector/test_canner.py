import base64
import os

import pytest
from orjson import orjson

"""
The Canner Enterprise must setup below:
- A user with PAT
- A data source with TPCH Tiny
- A workspace
- A TPCH table `orders` in the workspace
- The table `orders` must be with a description `This is a table comment`
- The table `orders` must have a column `o_comment` with a description `This is a comment`
"""

pytestmark = pytest.mark.canner

base_url = "/v2/connector/canner"

connection_info = {
    "host": os.getenv("CANNER_HOST", default="localhost"),
    "port": os.getenv("CANNER_PORT", default="7432"),
    "user": os.getenv("CANNER_USER", default="canner"),
    "pat": os.getenv("CANNER_PAT", default="PAT"),
    "workspace": os.getenv("CANNER_WORKSPACE", default="ws"),
}

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "refSql": f"select * from canner.{connection_info['workspace']}.orders",
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
                    "expression": "o_orderkey || '_' || o_custkey",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "cast('2024-01-01 23:59:59' as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "cast('2024-01-01 23:59:59 UTC' as timestamp with time zone)",
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "cast(NULL as timestamp)",
                    "type": "timestamp",
                },
            ],
            "primaryKey": "orderkey",
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_query(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
        },
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        1,
        370,
        "O",
        "172799.49",
        "1996-01-02",
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 UTC",
        None,
    ]
    assert result["dtypes"] == {
        "orderkey": "int64",
        "custkey": "int64",
        "orderstatus": "string",
        "totalprice": "double",
        "orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[ms]",
        "timestamptz": "timestamp[ms]",
        "test_null_time": "timestamp[ms]",
    }


async def test_query_with_connection_url(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": _to_connection_url()},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["dtypes"] is not None


async def test_query_with_limit(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        params={"limit": 1},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders"',
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
            "sql": 'SELECT * FROM "Orders" LIMIT 10',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1


async def test_query_without_manifest(client):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "manifestStr"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_without_sql(client, manifest_str):
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
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "connectionInfo"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_with_dry_run(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 204, response.text


async def test_query_with_dry_run_and_invalid_sql(client, manifest_str):
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


async def test_dry_plan(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/dry-plan",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT orderkey, order_cust_key FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 200
    assert response.text is not None


async def test_metadata_list_tables(client):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    result = next(filter(lambda x: x["name"] == "orders", response.json()))
    assert result["name"] == "orders"
    assert result["primaryKey"] is not None
    assert result["description"] == "This is a table comment"
    assert result["properties"]["catalog"] == "canner"
    assert result["properties"]["schema"] == "tpch_tiny"
    assert result["properties"]["table"] == "orders"
    assert len(result["columns"]) == 9
    comment_column = next(filter(lambda x: x["name"] == "o_comment", result["columns"]))
    assert comment_column["description"] == "This is a comment"


async def test_metadata_list_constraints(client):
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.json() == []


async def test_metadata_db_version(client):
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text is not None


def _to_connection_url():
    info = connection_info
    return f"postgres://{info['user']}:{info['pat']}@{info['host']}:{info['port']}/{info['workspace']}"
