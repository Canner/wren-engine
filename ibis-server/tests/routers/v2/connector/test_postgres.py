import base64
from urllib.parse import quote_plus, urlparse

import orjson
import pandas as pd
import psycopg2
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

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


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


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
        "2024-01-01 23:59:59.000000 UTC",
        None,
        "616263",
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "object",
        "totalprice": "object",
        "orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "object",
        "timestamptz": "object",
        "test_null_time": "datetime64[ns]",
        "bytea_column": "object",
    }


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


async def test_format_floating(client, manifest_str, postgres):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
SELECT
    0.0123e-5 AS case_scientific_original,
    1.23e+4 AS case_scientific_positive,
    -4.56e-3 AS case_scientific_negative,
    7.89e0 AS case_scientific_zero_exponent,
    0e0 AS case_scientific_zero,

    123.456 AS case_decimal_positive,
    -123.456 AS case_decimal_negative,
    0.0000123 AS case_decimal_small,
    123.0000 AS case_decimal_trailing_zeros,
    0.0 AS case_decimal_zero,

    0 AS case_integer_zero,
    0e-9 AS case_integer_zero_scientific,
    -1 AS case_integer_negative,
    9999999999 AS case_integer_large,

    1.7976931348623157E+308 AS case_float_max,
    2.2250738585072014E-308 AS case_float_min,
    -1.7976931348623157E+308 AS case_float_min_negative,

    1.23e4 + 4.56 AS case_mixed_addition,
    -1.23e-4 - 123.45 AS case_mixed_subtraction,
    0.0123e-5 * 1000 AS case_mixed_multiplication,
    123.45 / 1.23e2 AS case_mixed_division,

    CAST('NaN' AS FLOAT) AS case_special_nan,
    CAST('Infinity' AS FLOAT) AS case_special_infinity,
    CAST('-Infinity' AS FLOAT) AS case_special_negative_infinity,
    NULL AS case_special_null,

    CAST(123.456 AS FLOAT) AS case_cast_float,
    CAST(1.23e4 AS DECIMAL(10,5)) AS case_cast_decimal
            """,
        },
    )
    assert response.status_code == 200
    result = response.json()

    assert result["data"][0][0] == "1.23E-7"
    assert result["data"][0][1] == "1.23E+4"
    assert result["data"][0][2] == "-0.00456"
    assert result["data"][0][3] == "7.89"
    assert result["data"][0][4] == "0"
    assert result["data"][0][5] == "123.456"
    assert result["data"][0][6] == "-123.456"
    assert result["data"][0][7] == "0.0000123"
    assert result["data"][0][8] == "123"
    assert result["data"][0][9] == "0"
    assert result["data"][0][10] == 0
    assert result["data"][0][11] == "0"
    assert result["data"][0][12] == -1
    assert result["data"][0][13] == 9999999999
    assert result["data"][0][14] == "1.7976931348623157E+308"
    assert result["data"][0][15] == "2.2250738585072014E-308"
    assert result["data"][0][16] == "-1.7976931348623157E+308"
    assert result["data"][0][17] == "12304.56"
    assert result["data"][0][18] == "-123.450123"
    assert result["data"][0][19] == "0.000123"
    assert result["data"][0][20] == "1.0036585365853659"
    assert result["data"][0][21] == "nan"
    assert result["data"][0][22] == "inf"
    assert result["data"][0][23] == "-inf"
    assert result["data"][0][24] is None
    assert result["data"][0][25] == "123.456001"
    assert result["data"][0][26] == "12300.00000"


async def test_dry_run_with_connection_url_and_password_with_bracket_should_not_raise_value_error(
    client, manifest_str, postgres: PostgresContainer
):
    connection_url = _to_connection_url(postgres)
    part = urlparse(connection_url)
    password_with_bracket = quote_plus(f"{part.password}[")
    connection_url = part._replace(
        netloc=f"{part.username}:{password_with_bracket}@{part.hostname}:{part.port}"
    ).geturl()

    with pytest.raises(
        psycopg2.OperationalError,
        match='FATAL:  password authentication failed for user "test"',
    ):
        await client.post(
            url=f"{base_url}/query",
            params={"dryRun": True},
            json={
                "connectionInfo": {"connectionUrl": connection_url},
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )


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
        response.text == f"The rule `unknown_rule` is not in the rules, rules: {rules}"
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
    assert response.text == "Missing required parameter: `columnName`"

    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"columnName": "orderkey"},
        },
    )
    assert response.status_code == 422
    assert response.text == "Missing required parameter: `modelName`"


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


async def test_model_substitute(client, manifest_str, postgres: PostgresContainer):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/model-substitute",
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


async def test_model_substitute_with_cte(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                WITH orders_cte AS (
                    SELECT * FROM "public"."orders"
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
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                SELECT * FROM (
                    SELECT * FROM "public"."orders"
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
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Nation" LIMIT 1',
        },
    )
    assert response.status_code == 422
    assert response.text == 'Model not found: "Nation"'


async def test_model_substitute_non_existent_column(
    client, manifest_str, postgres: PostgresContainer
):
    connection_info = _to_connection_info(postgres)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT x FROM "public"."orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    assert 'column "x" does not exist' in response.text


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
