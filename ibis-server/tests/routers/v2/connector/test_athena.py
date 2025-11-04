import base64
import os

import orjson
import pytest

pytestmark = pytest.mark.athena

base_url = "/v2/connector/athena"

connection_info = {
    "s3_staging_dir": os.getenv("TEST_ATHENA_S3_STAGING_DIR"),
    "aws_access_key_id": os.getenv("TEST_ATHENA_AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("TEST_ATHENA_AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("TEST_ATHENA_REGION_NAME", "ap-northeast-1"),
    "schema_name": "test",
}

# Manifest for the database create from glue
glue_connection_info = {
    "s3_staging_dir": os.getenv("TEST_ATHENA_S3_STAGING_DIR"),
    "aws_access_key_id": os.getenv("TEST_ATHENA_AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("TEST_ATHENA_AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("TEST_ATHENA_REGION_NAME", "ap-northeast-1"),
    "schema_name": "wren-engine-glue-test",
}

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "dataSource": "athena",
    "models": [
        {
            "name": "Orders",
            "refSql": "select * from test.orders",
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
                    "expression": "concat(cast(o_orderkey as varchar), '_', cast(o_custkey as varchar))",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "TIMESTAMP '2024-01-01 23:59:59'",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "CAST(TIMESTAMP '2024-01-01 23:59:59 UTC' AS timestamp)",
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
        36901,
        "O",
        "173665.47",
        "1996-01-02",
        "1_36901",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000",
        None,
        "616263",
    ]
    assert result["dtypes"] == {
        "orderkey": "int64",
        "custkey": "int64",
        "orderstatus": "string",
        "totalprice": "decimal128(38, 9)",
        "orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[us]",
        "timestamptz": "timestamp[us]",
        "test_null_time": "timestamp[us]",
        "bytea_column": "binary",
    }


async def test_query_glue_database(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": glue_connection_info,  # Use the Glue connection info
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
        36901,
        "O",
        "173665.47",
        "1996-01-02",
        "1_36901",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000",
        None,
        "616263",
    ]
    assert result["dtypes"] == {
        "orderkey": "int64",
        "custkey": "int64",
        "orderstatus": "string",
        "totalprice": "decimal128(38, 9)",
        "orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[us]",
        "timestamptz": "timestamp[us]",
        "test_null_time": "timestamp[us]",
        "bytea_column": "binary",
    }


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


async def test_metadata_list_tables(client):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) >= 1

    # Check if our test table exists
    test_table = next(filter(lambda t: "orders" in t["name"].lower(), tables), None)
    if test_table:
        assert test_table["name"] is not None
        assert test_table["properties"] is not None
        assert test_table["properties"]["schema"] == connection_info["schema_name"]
        assert len(test_table["columns"]) > 0

        # Check column structure
        column = test_table["columns"][0]
        assert column["name"] is not None
        assert column["type"] is not None
        assert "notNull" in column


async def test_metadata_list_constraints(client):
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    result = response.json()
    # Athena doesn't support foreign key constraints, so should return empty list
    assert len(result) == 0


async def test_metadata_db_version(client):
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text is not None
    # Should return the AWS Athena version string
    assert "AWS Athena" in response.text
