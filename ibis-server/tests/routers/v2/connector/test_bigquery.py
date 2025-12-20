import base64
import os
import time

import orjson
import pytest

pytestmark = pytest.mark.bigquery

base_url = "/v2/connector/bigquery"

connection_info = {
    "project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
    "dataset_id": "tpch_tiny",
    "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
}

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "refSql": "select * from tpch_tiny.orders",
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
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "cast(NULL as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "bytea_column",
                    "expression": "cast('abc' as bytea)",
                    "type": "bytea",
                },
            ],
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
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        1,
        370,
        "O",
        172799.49,
        "1996-01-02",
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 +00:00",
        None,
        "616263",
    ]
    assert result["dtypes"] == {
        "orderkey": "int64",
        "custkey": "int64",
        "orderstatus": "string",
        "totalprice": "double",
        "orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[us]",
        "timestamptz": "timestamp[us, tz=UTC]",
        "test_null_time": "timestamp[us]",
        "bytea_column": "binary",
    }


async def test_query_with_cache(client, manifest_str):
    # add random timestamp to the query to ensure cache is not hit
    now = int(time.time())
    sql = f'SELECT *, {now} FROM "Orders" ORDER BY orderkey LIMIT 1'
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
    assert int(response2.headers["X-Cache-Create-At"]) > 1743984000  # 2025.04.07
    result2 = response2.json()

    assert result1["dtypes"] == result2["dtypes"]


async def test_query_with_cache_override(client, manifest_str):
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
        },
    )
    assert response1.status_code == 200

    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true&overrideCache=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
        },
    )

    assert response2.status_code == 200
    assert response2.headers["X-Cache-Override"] == "true"
    assert int(response2.headers["X-Cache-Override-At"]) > int(
        response2.headers["X-Cache-Create-At"]
    )


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
    assert response.status_code == 204


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


async def test_query_values(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM (VALUES (1, 2), (3, 4))",
        },
    )

    assert response.status_code == 204


async def test_scientific_notation(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT cast(0 as numeric) as col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"][0] == ["0"]


async def test_query_empty_json(client, manifest_str):
    """Test the empty result with json column."""
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "connectionInfo": connection_info,
            "sql": "select json_object('a', 1, 'b', 2) limit 0",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 0
    assert result["dtypes"] == {"f0_": "string"}

    """Test only the json column is null."""
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "connectionInfo": connection_info,
            "sql": "select cast(null as JSON), 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert result["data"][0][0] is None
    assert result["data"][0][1] == 1
    assert result["dtypes"] == {"f0_": "string", "f1_": "int64"}


async def test_interval(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT INTERVAL '1' YEAR + INTERVAL '100' MONTH + INTERVAL '100' DAY + INTERVAL '1' HOUR AS col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"][0] == ["112 mons 100 days 1 hours"]
    assert result["dtypes"] == {"col": "month_day_nano_interval"}


async def test_avg_interval(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT AVG(DATE '2024-01-01' - orderdate) AS col from \"Orders\"",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"][0] == ["10484 days 8 hours 54 mins 14.400000000 secs"]
    assert result["dtypes"] == {"col": "month_day_nano_interval"}


async def test_custom_datatypes_no_overrides(client, manifest_str):
    # Trigger import the official BigQueryType
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "connectionInfo": connection_info,
            "sql": "select json_object('a', 1, 'b', 2) limit 0",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 0
    assert result["dtypes"] == {"f0_": "string"}

    # Should use back the custom BigQueryType
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT INTERVAL '1' YEAR + INTERVAL '100' MONTH + INTERVAL '100' DAY + INTERVAL '1' HOUR AS col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"][0] == ["112 mons 100 days 1 hours"]
    assert result["dtypes"] == {"col": "month_day_nano_interval"}


async def test_metadata_list_tables(client):
    def _assert_nested_column(column):
        if column["nestedColumns"] is not None:
            for nested_column in column["nestedColumns"]:
                assert ".".join(nested_column["name"].split(".")[:-1]) == column["name"]
                _assert_nested_column(nested_column)

    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    res = response.json()

    # assert nest_column is correct
    for table in res:
        for column in table["columns"]:
            _assert_nested_column(column)


async def test_metadata_list_constraints(client):
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200


async def test_metadata_list_unsupported_project_connection(client):
    multi_dataset_connection_info = {
        "bigquery_type": "project",
        "billing_project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
        "region": os.getenv("TEST_BIG_QUERY_REGION", "asia-east1"),
        "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
    }
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": multi_dataset_connection_info},
    )
    assert response.status_code == 422
    assert (
        "BigQuery project-level connection info is only supported by v3 API for metadata table list retrieval."
        in response.text
    )


async def test_metadata_db_version(client):
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text == '"Follow BigQuery release version"'


async def test_order_by_nulls_last(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT letter FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num, letter) ORDER BY num",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 3
    assert result["data"][0][0] == "one"
    assert result["data"][1][0] == "two"
    assert result["data"][2][0] == "three"
