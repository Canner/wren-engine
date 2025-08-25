import base64
from urllib.parse import quote_plus, urlparse

import geopandas as gpd
import orjson
import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

from app.model.data_source import X_WREN_DB_STATEMENT_TIMEOUT
from app.model.error import ErrorCode, ErrorPhase
from app.model.validator import rules
from tests.conftest import file_path

pytestmark = pytest.mark.postgres

base_url = "/v2/connector/postgres"

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "catalog": "test",
                "schema": "public",
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
            "primaryKey": "orderkey",
        },
        {
            "name": "Customer",
            "refSql": "SELECT * FROM public.customer",
            "columns": [
                {
                    "name": "custkey",
                    "type": "integer",
                    "expression": "c_custkey",
                },
                {
                    "name": "orders",
                    "type": "Orders",
                    "relationship": "CustomerOrders",
                },
                {
                    "name": "orders_key",
                    "type": "varchar",
                    "isCalculated": True,
                    "expression": "orders.orderkey",
                },
            ],
        },
    ],
    "relationships": [
        {
            "name": "CustomerOrders",
            "models": ["Customer", "Orders"],
            "joinType": "ONE_TO_MANY",
            "condition": "Customer.custkey = Orders.custkey",
        }
    ],
}

manifest_model_without_catalog = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "schema": "public",
                "table": "orders",
            },
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
            ],
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def manifest_without_catalog_str():
    return base64.b64encode(orjson.dumps(manifest_model_without_catalog)).decode(
        "utf-8"
    )


@pytest.fixture(scope="module")
def postgres(request) -> PostgresContainer:
    pg = PostgresContainer("postgres:16-alpine").start()
    engine = sqlalchemy.create_engine(pg.get_connection_url())
    pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
        "customer", engine, index=False
    )
    with engine.begin() as conn:
        conn.execute(text("COMMENT ON TABLE orders IS 'This is a table comment'"))
        conn.execute(text("COMMENT ON COLUMN orders.o_comment IS 'This is a comment'"))
    request.addfinalizer(pg.stop)
    return pg


# PostGIS only provides the amd64 images. To run the PostGIS tests on ARM devices,
# please manually download the image by using the command below.
#   docker pull --platform linux/amd64 postgis/postgis:16-3.5-alpine
@pytest.fixture(scope="module")
def postgis(request) -> PostgresContainer:
    pg = PostgresContainer("postgis/postgis:16-3.5-alpine").start()
    engine = sqlalchemy.create_engine(pg.get_connection_url())
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    gpd.read_parquet(
        file_path("resource/tpch/data/cities_geometry.parquet")
    ).to_postgis("cities_geometry", engine, index=False)
    request.addfinalizer(pg.stop)
    return pg


async def test_query(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
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
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 +00:00",
        None,
        "616263",
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "string",
        "totalprice": "string",
        "orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[us]",
        "timestamptz": "timestamp[us, tz=UTC]",
        "test_null_time": "timestamp[us]",
        "bytea_column": "binary",
    }


async def test_query_with_cache(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)

    sql = 'SELECT * FROM "Orders" LIMIT 10'
    # First request - should miss cache
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": sql,
        },
    )

    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()

    # Second request with same SQL - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
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

    # Verify results are identical
    assert result1["data"] == result2["data"]
    assert result1["columns"] == result2["columns"]
    assert result1["dtypes"] == result2["dtypes"]


async def test_query_with_cache_override(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    # First request - should miss cache then create cache
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 10',
        },
    )
    assert response1.status_code == 200

    # Second request with same SQL - should hit cache and override it
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true&overrideCache=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 10',
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Override"] == "true"
    assert int(response2.headers["X-Cache-Override-At"]) > int(
        response2.headers["X-Cache-Create-At"]
    )


async def test_query_with_connection_url(
    client, manifest_str, postgres: PostgresContainer
):
    connection_url = _to_connection_url(postgres)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0][0] == 1
    assert result["dtypes"] is not None


async def test_query_with_connection_url_and_cache_enable(
    client, manifest_str, postgres: PostgresContainer
):
    connection_url = _to_connection_url(postgres)
    # First request - should miss cache then create cache
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )

    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()

    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"
    assert int(response2.headers["X-Cache-Create-At"]) > 1743984000  # 2025.04.07
    result2 = response2.json()

    # Verify results are identical
    assert result1["data"] == result2["data"]


async def test_query_with_connection_url_and_cache_override(
    client, manifest_str, postgres: PostgresContainer
):
    connection_url = _to_connection_url(postgres)
    # First request - should miss cache then create cache
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response1.status_code == 200

    # Second request with same SQL - should hit cache and override it
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true&overrideCache=true",  # Enable cache
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Override"] == "true"
    assert int(response2.headers["X-Cache-Override-At"]) > int(
        response2.headers["X-Cache-Create-At"]
    )


async def test_query_with_dot_all(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    test_sqls = [
        'SELECT "Customer".* FROM "Customer"',
        'SELECT c.* FROM "Customer" AS c',
        'SELECT c.* FROM "Customer" AS c JOIN "Orders" AS o ON c.custkey = o.custkey',
    ]
    for sql in test_sqls:
        response = await client.post(
            url=f"{base_url}/query",
            params={"limit": 1},
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "sql": sql,
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == 1  # Not include calculated column
        assert len(result["data"]) == 1
        assert result["dtypes"] is not None


async def test_limit_pushdown(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/query",
        params={"limit": 10},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 100',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 10

    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/query",
        params={"limit": 10},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT count(*) FILTER (where orderkey > 10) FROM "Orders" LIMIT 100',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1


async def test_dry_run_with_connection_url_and_password_with_bracket_should_not_raise_value_error(
    client, manifest_str, postgres: PostgresContainer
):
    connection_url = _to_connection_url(postgres)
    part = urlparse(connection_url)
    password_with_bracket = quote_plus(f"{part.password}[")
    connection_url = part._replace(
        netloc=f"{part.username}:{password_with_bracket}@{part.hostname}:{part.port}"
    ).geturl()

    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )

    assert response.status_code == 422
    result = response.json()
    assert result["errorCode"] == ErrorCode.GET_CONNECTION_ERROR.name
    assert 'password authentication failed for user "test"' in result["message"]


async def test_query_with_limit(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
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


async def test_query_with_invalid_manifest_str(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": "xxx",
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    assert (
        "com.fasterxml.jackson.core.JsonParseException: Unexpected character"
        in response.text
    )


async def test_query_without_manifest(client, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
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


async def test_query_without_sql(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
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


async def test_query_with_dry_run(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
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


async def test_query_with_dry_run_and_invalid_sql(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
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


async def test_query_with_keyword_filter(client, manifest_str, postgres):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT count(*) FILTER(WHERE orderkey IS NOT NULL) FROM "Orders"',
        },
    )
    assert response.status_code == 200


async def test_validate_with_unknown_rule(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/unknown_rule",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 404
    assert (
        response.json()["message"]
        == f"The rule `unknown_rule` is not in the rules, rules: {rules}"
    )


async def test_validate_rule_column_is_valid(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 204


async def test_validate_rule_column_is_valid_with_invalid_parameters(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "X", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 422

    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders", "columnName": "X"},
        },
    )
    assert response.status_code == 422


async def test_validate_rule_column_is_valid_without_parameters(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={"connectionInfo": connection_info, "manifestStr": manifest_str},
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "parameters"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_validate_rule_column_is_valid_without_one_parameter(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders"},
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["errorCode"] == ErrorCode.VALIDATION_PARAMETER_ERROR.name
    assert result["message"] == "columnName is required"
    assert result["phase"] == ErrorPhase.VALIDATION.name

    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"columnName": "orderkey"},
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == "modelName is required"


async def test_metadata_list_tables(client, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    result = next(filter(lambda x: x["name"] == "public.orders", response.json()))
    assert result["name"] == "public.orders"
    assert result["primaryKey"] is not None
    assert result["description"] == "This is a table comment"
    assert result["properties"] == {
        "catalog": "test",
        "schema": "public",
        "table": "orders",
        "path": None,
    }
    assert len(result["columns"]) == 9
    assert result["columns"][8] == {
        "name": "o_comment",
        "nestedColumns": None,
        "type": "TEXT",
        "notNull": False,
        "description": "This is a comment",
        "properties": None,
    }


async def test_metadata_list_constraints(client, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200


async def test_metadata_db_version(client, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert "PostgreSQL 16" in response.text


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


async def test_model_substitute(
    client, manifest_str, manifest_without_catalog_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "test"."public"."orders"',
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\""'
    )

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-catalog": "test", "x-user-schema": "public"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\""'
    )

    # Test only have x-user-catalog
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-catalog": "test"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 404

    # Test only have x-user-catalog but have schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-catalog": "test"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "public"."orders"',
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\""'
    )

    # Test only have x-user-schema
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-schema": "public"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 404

    # Test only have x-user-schema
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-schema": "public"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_without_catalog_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 200

    # Test only have x-user-schema
    response = await client.post(
        url=f"{base_url}/model-substitute",
        # empty catalog
        headers={"x-user-catalog": "", "x-user-schema": "public"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_without_catalog_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 200


async def test_model_substitute_with_cte(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                WITH orders_cte AS (
                    SELECT * FROM "test"."public"."orders"
                )
                SELECT * FROM orders_cte;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"WITH orders_cte AS (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") SELECT * FROM orders_cte"'
    )

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-catalog": "test", "x-user-schema": "public"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                WITH orders_cte AS (
                    SELECT * FROM "orders"
                )
                SELECT * FROM orders_cte;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"WITH orders_cte AS (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") SELECT * FROM orders_cte"'
    )


async def test_model_substitute_with_subquery(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                SELECT * FROM (
                    SELECT * FROM "test"."public"."orders"
                ) AS orders_subquery;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") AS orders_subquery"'
    )

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-catalog": "test", "x-user-schema": "public"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                SELECT * FROM (
                    SELECT * FROM "orders"
                ) AS orders_subquery;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") AS orders_subquery"'
    )


async def test_model_substitute_out_of_scope(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Nation" LIMIT 1',
        },
    )
    assert response.status_code == 404
    assert response.json()["message"] == 'Model not found: "Nation"'

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-catalog": "test", "x-user-schema": "public"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Nation" LIMIT 1',
        },
    )
    assert response.status_code == 404
    assert response.json()["message"] == 'Model not found: "Nation"'


async def test_model_substitute_non_existent_column(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT x FROM "test"."public"."orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert 'column "x" does not exist' in result["message"]

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={"x-user-catalog": "test", "x-user-schema": "public"},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT x FROM "orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert 'column "x" does not exist' in result["message"]


async def test_postgis_geometry(client, manifest_str, postgis: PostgresContainer):
    connection_info = _to_connection_info(postgis)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": (
                "SELECT ST_Distance(a.geometry, b.geometry) AS distance "
                "FROM cities_geometry a, cities_geometry b "
                "WHERE a.\"City\" = 'London' AND b.\"City\" = 'New York'"
            ),
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"][0] == [74.66265347816136]


async def test_decimal_precision(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
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


async def test_connection_timeout(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    # Set a very short timeout to force a timeout error
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT 1 FROM (SELECT pg_sleep(5))",  # This will take longer than the default timeout
        },
        headers={X_WREN_DB_STATEMENT_TIMEOUT: "1"},  # Set timeout to 1 second
    )
    assert response.status_code == 504  # Gateway Timeout
    result = response.json()
    assert result["errorCode"] == ErrorCode.DATABASE_TIMEOUT.name
    assert (
        "Query was cancelled: canceling statement due to statement timeout"
        in result["message"]
    )


async def test_uuid_type(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "select '123e4567-e89b-12d3-a456-426614174000'::uuid as order_uuid",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert result["data"][0][0] == "123e4567-e89b-12d3-a456-426614174000"


async def test_order_by_nulls_last(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
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


def _to_connection_info(pg: PostgresContainer):
    return {
        "host": pg.get_container_host_ip(),
        "port": pg.get_exposed_port(pg.port),
        "user": pg.username,
        "password": pg.password,
        "database": pg.dbname,
    }


def _to_connection_url(pg: PostgresContainer):
    info = _to_connection_info(pg)
    return f"postgres://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"
