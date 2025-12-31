import base64
import os
import time
from re import split

import orjson
import pytest

from app.dependencies import X_WREN_FALLBACK_DISABLE, X_WREN_VARIABLE_PREFIX
from app.model.metadata.bigquery import BIGQUERY_PUBLIC_DATASET_PROJECT_ID
from tests.routers.v3.connector.bigquery.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "dataSource": "bigquery",
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
        {
            "name": "customer",
            "tableReference": {
                "schema": "tpch_tiny",
                "table": "customer",
            },
            "columns": [
                {"name": "c_custkey", "type": "integer"},
                {
                    "name": "c_name",
                    "type": "varchar",
                    "columnLevelAccessControl": {
                        "name": "c_name_access",
                        "requiredProperties": [
                            {
                                "name": "session_level",
                                "required": False,
                            }
                        ],
                        "operator": "EQUALS",
                        "threshold": "1",
                    },
                },
            ],
            "rowLevelAccessControls": [
                {
                    "name": "customer_access",
                    "requiredProperties": [
                        {
                            "name": "session_user",
                            "required": False,
                        }
                    ],
                    "condition": "c_name = @session_user",
                },
            ],
            "primaryKey": "c_custkey",
        },
        {
            "name": "null_test",
            "tableReference": {
                "schema": "engine_ci",
                "table": "null_test",
            },
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "letter", "type": "varchar"},
            ],
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
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        36485,
        1202,
        "F",
        356711.63,
        "1992-06-06",
        "36485_1202",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 +00:00",
        "2024-01-16 04:00:00.000000 +00:00",  # utc-5
        "2024-07-16 03:00:00.000000 +00:00",  # utc-4
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

    multi_dataset_connection_info = {
        "bigquery_type": "project",
        "billing_project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
        "region": os.getenv("TEST_BIG_QUERY_REGION", "asia-east1"),
        "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": multi_dataset_connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1


async def test_query_cross_project_dataset(client):
    manifest = {
        "catalog": "wren",
        "schema": "public",
        "dataSource": "bigquery",
        "models": [
            {
                "name": "orders",
                "tableReference": {
                    "catalog": "wrenai",
                    "schema": "tpch_tiny_us",
                    "table": "orders",
                },
                "columns": [
                    {
                        "name": "o_orderkey",
                        "type": "integer",
                    },
                    {
                        "name": "o_custkey",
                        "type": "integer",
                    },
                ],
            },
            {
                "name": "311_service_requests",
                "tableReference": {
                    "catalog": "bigquery-public-data",
                    "schema": "austin_311",
                    "table": "311_service_requests",
                },
                "columns": [
                    {
                        "name": "city",
                        "type": "string",
                    },
                    {
                        "name": "unique_key",
                        "type": "string",
                    },
                ],
            },
        ],
    }

    manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")

    connection_info = {
        "bigquery_type": "project",
        "billing_project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
        "region": "US",
        "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT o.o_orderkey, s.city FROM orders o CROSS JOIN "311_service_requests" s LIMIT 2',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 2


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
        "1970-01-01 00:16:40.000000 +00:00",
        "1970-01-01 00:00:01.000000 +00:00",
        "1970-01-12 13:46:40.000000 +00:00",
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


async def test_decimal_precision(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT cast(1 as decimal(38, 8)) / cast(3 as decimal(38, 8)) as result",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert result["data"][0][0] == "0.333333333"


async def test_order_by_nulls_last(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT letter FROM null_test ORDER BY id",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 3
    assert result["data"][0][0] == "one"
    assert result["data"][1][0] == "two"
    assert result["data"][2][0] == "three"

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT letter FROM null_test ORDER BY id desc",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 3
    assert result["data"][0][0] == "two"
    assert result["data"][1][0] == "one"
    assert result["data"][2][0] == "three"


async def test_count(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT COUNT(*) FROM wren.public.orders",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert (
        result["data"][0][0] == 15000
    )  # Adjust based on actual data count in orders table
    assert result["dtypes"] == {"count_40_42_41": "int64"}


async def test_cache_with_different_wren_variables(
    client, manifest_str, connection_info
):
    # First request with session_user = 'Customer#000000001'
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000001'",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"  # Cache miss on first request
    result1 = response1.json()

    # Second request with same session_user - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000001'",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"  # Should hit cache
    result2 = response2.json()
    assert result1["data"] == result2["data"]

    # Third request with different session_user - should miss cache and create new entry
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000002'",
        },
    )
    assert response3.status_code == 200
    assert (
        response3.headers["X-Cache-Hit"] == "false"
    )  # Should miss cache due to different header


async def test_cache_with_different_session_levels(
    client, manifest_str, connection_info
):
    # First request with session_level = 1
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()

    # Second request with same session_level - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"
    assert result1["data"] == response2.json()["data"]

    # Third request with different session_level - should miss cache
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "2",
        },
    )
    assert response3.status_code == 200
    assert (
        response3.headers["X-Cache-Hit"] == "false"
    )  # Different header should miss cache


async def test_cache_ignores_irrelevant_headers(client, manifest_str, connection_info):
    # First request with user-agent header
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            "User-Agent": "TestClient/1.0",
            "X-Request-ID": "request-123",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"

    # Second request with different irrelevant headers - should still hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            "User-Agent": "TestClient/2.0",
            "X-Request-ID": "request-456",
            "Accept": "application/json",
        },
    )
    assert response2.status_code == 200
    assert (
        response2.headers["X-Cache-Hit"] == "true"
    )  # Should hit cache despite different irrelevant headers


async def test_metadata_list_schemas(client, project_connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/schemas",
        json={
            "connectionInfo": project_connection_info,
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert isinstance(result, list)
    # one project
    assert len(result) == 1
    # multiple datasets
    assert len(result[0]["schemas"]) > 1
    assert any(schema == "tpch_tiny" for schema in result[0]["schemas"])
    assert any(schema == "tpch_sf1" for schema in result[0]["schemas"])
    assert len(result[0]["schemas"]) > 2

    response = await client.post(
        url=f"{base_url}/metadata/schemas",
        json={
            "connectionInfo": {
                "bigquery_type": "project",
                "billing_project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
                "region": "US",
                "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
            },
            "filterInfo": {
                "projects": [{"projectId": BIGQUERY_PUBLIC_DATASET_PROJECT_ID}]
            },
        },
    )

    result = response.json()
    assert len(result) == 2
    # multiple datasets
    public_project = next(
        project
        for project in result
        if project["name"] == BIGQUERY_PUBLIC_DATASET_PROJECT_ID
    )
    assert any(schema == "austin_311" for schema in public_project["schemas"])


async def test_metadata_list_tables(client, project_connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": project_connection_info,
            "filterInfo": {
                "projects": [
                    {
                        "projectId": "wrenai",
                        "datasetIds": ["tpch_tiny"],
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    assert len(response.json()) == 8
    table_name = response.json()[0]["name"]
    assert len(split(r"\.", table_name)) == 1  # no catalog and schema in table name

    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": project_connection_info,
            "filterInfo": {
                "projects": [
                    {
                        "projectId": "wrenai",
                        "datasetIds": ["tpch_tiny", "tpch_sf1"],
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    assert len(response.json()) == 16
    table_name = response.json()[0]["name"]
    assert len(split(r"\.", table_name)) == 2  # no catalog in table name


async def test_metadata_list_public_dataset_tables(client):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": {
                "bigquery_type": "project",
                "billing_project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
                "region": "asia-east1",
                "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
            },
            "filterInfo": {
                "projects": [
                    {
                        "projectId": BIGQUERY_PUBLIC_DATASET_PROJECT_ID,
                        "datasetIds": ["austin_311"],
                    }
                ]
            },
        },
    )

    assert response.status_code == 422
    assert (
        f"Dataset {BIGQUERY_PUBLIC_DATASET_PROJECT_ID}.austin_311 is in region us, which does not match the connection region asia-east1."
        in response.text
    )

    us_connection_info = {
        "bigquery_type": "project",
        "billing_project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
        "region": "US",
        "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
    }

    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": us_connection_info,
            "filterInfo": {
                "projects": [
                    {
                        "projectId": BIGQUERY_PUBLIC_DATASET_PROJECT_ID,
                        "datasetIds": ["austin_311"],
                    }
                ]
            },
        },
    )

    assert response.status_code == 200
    assert len(response.json()) > 0


async def test_metadata_list_tables_missing_field(client, project_connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": project_connection_info,
            "filterInfo": {"projects": []},
        },
    )

    assert response.status_code == 422
    assert "At least one project and dataset must be specified" in response.text

    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": project_connection_info,
            "filterInfo": {
                "projects": [
                    {
                        "projectId": "",
                    }
                ],
            },
        },
    )

    assert response.status_code == 422
    assert "project id should not be empty" in response.text

    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": project_connection_info,
            "filterInfo": {
                "projects": [
                    {
                        "projectId": "",
                    }
                ],
            },
        },
    )

    assert response.status_code == 422
    assert "project id should not be empty" in response.text

    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": project_connection_info,
            "filterInfo": {
                "projects": [
                    {
                        "projectId": "wrenai",
                        "datasetIds": [],
                    }
                ],
            },
        },
    )

    assert response.status_code == 422
    assert "dataset ids should not be empty" in response.text

    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": project_connection_info,
            "filterInfo": {
                "projects": [
                    {
                        "projectId": "wrenai",
                        "datasetIds": [""],
                    }
                ],
            },
        },
    )

    assert response.status_code == 422
    assert "dataset id should not be empty" in response.text

    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": project_connection_info,
            "filterInfo": {
                "projects": [
                    {
                        "projectId": "wrenai",
                        "datasetIds": ["tpch_sf1", ""],
                    }
                ],
            },
        },
    )

    assert response.status_code == 422
    assert "dataset id should not be empty" in response.text
