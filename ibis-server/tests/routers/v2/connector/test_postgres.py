import base64
from urllib.parse import quote_plus, urlparse

import orjson
import pandas as pd
import psycopg2
import pytest
import sqlalchemy
from fastapi.testclient import TestClient
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

from app.main import app
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
            "refSql": "select * from public.orders",
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


@pytest.fixture
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def postgres(request) -> PostgresContainer:
    pg = PostgresContainer("postgres:16.4-alpine").start()
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


with TestClient(app) as client:

    def test_query(manifest_str, postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
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

    def test_query_with_connection_url(manifest_str, postgres: PostgresContainer):
        connection_url = _to_connection_url(postgres)
        response = client.post(
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

    def test_query_with_dot_all(manifest_str, postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        test_sqls = [
            'SELECT "Customer".* FROM "Customer"',
            'SELECT c.* FROM "Customer" AS c',
            'SELECT c.* FROM "Customer" AS c JOIN "Orders" AS o ON c.custkey = o.custkey',
        ]
        for sql in test_sqls:
            response = client.post(
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

    def test_dry_run_with_connection_url_and_password_with_bracket_should_not_raise_value_error(
        manifest_str, postgres: PostgresContainer
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
            client.post(
                url=f"{base_url}/query",
                params={"dryRun": True},
                json={
                    "connectionInfo": {"connectionUrl": connection_url},
                    "manifestStr": manifest_str,
                    "sql": 'SELECT * FROM "Orders" LIMIT 1',
                },
            )

    def test_query_with_limit(manifest_str, postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
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

        response = client.post(
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

    def test_query_without_manifest(postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
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

    def test_query_without_sql(manifest_str, postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/query",
            json={"connectionInfo": connection_info, "manifestStr": manifest_str},
        )
        assert response.status_code == 422
        result = response.json()
        assert result["detail"][0] is not None
        assert result["detail"][0]["type"] == "missing"
        assert result["detail"][0]["loc"] == ["body", "sql"]
        assert result["detail"][0]["msg"] == "Field required"

    def test_query_without_connection_info(manifest_str):
        response = client.post(
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

    def test_query_with_dry_run(manifest_str, postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 204

    def test_query_with_dry_run_and_invalid_sql(
        manifest_str, postgres: PostgresContainer
    ):
        connection_info = _to_connection_info(postgres)
        response = client.post(
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

    def test_validate_with_unknown_rule(manifest_str, postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/validate/unknown_rule",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 404
        assert (
            response.text
            == f"The rule `unknown_rule` is not in the rules, rules: {rules}"
        )

    def test_validate_rule_column_is_valid(manifest_str, postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 204

    def test_validate_rule_column_is_valid_with_invalid_parameters(
        manifest_str, postgres: PostgresContainer
    ):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "X", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 422

        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "X"},
            },
        )
        assert response.status_code == 422

    def test_validate_rule_column_is_valid_without_parameters(
        manifest_str, postgres: PostgresContainer
    ):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={"connectionInfo": connection_info, "manifestStr": manifest_str},
        )
        assert response.status_code == 422
        result = response.json()
        assert result["detail"][0] is not None
        assert result["detail"][0]["type"] == "missing"
        assert result["detail"][0]["loc"] == ["body", "parameters"]
        assert result["detail"][0]["msg"] == "Field required"

    def test_validate_rule_column_is_valid_without_one_parameter(
        manifest_str, postgres: PostgresContainer
    ):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "Orders"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `columnName`"

        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"columnName": "orderkey"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `modelName`"

    def test_metadata_list_tables(postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
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

    def test_metadata_list_constraints(postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/metadata/constraints",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200

    def test_metadata_db_version(postgres: PostgresContainer):
        connection_info = _to_connection_info(postgres)
        response = client.post(
            url=f"{base_url}/metadata/version",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200
        assert "PostgreSQL 16.4" in response.text

    def test_dry_plan(manifest_str):
        response = client.post(
            url=f"{base_url}/dry-plan",
            json={
                "manifestStr": manifest_str,
                "sql": 'SELECT orderkey, order_cust_key FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 200
        assert response.text is not None

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
