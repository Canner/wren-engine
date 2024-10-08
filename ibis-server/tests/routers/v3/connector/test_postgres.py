import base64

import orjson
import pandas as pd
import pytest
import sqlalchemy
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from app.main import app
from app.model.validator import rules
from tests.confest import file_path

pytestmark = pytest.mark.beta

client = TestClient(app)

base_url = "/v3/connector/postgres"

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
                    "type": "timestamp",
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
            ],
            "primaryKey": "c_custkey",
        },
    ],
}

manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


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
    request.addfinalizer(pg.stop)
    return pg


def test_query(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
    response = client.post(
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
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 UTC",
        "1_370",
        370,
        "1996-01-02",
        1,
        "O",
        "172799.49",
    ]
    assert result["dtypes"] == {
        "o_orderkey": "int32",
        "o_custkey": "int32",
        "o_orderstatus": "object",
        "o_totalprice": "object",
        "o_orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "object",
        "timestamptz": "object",
    }


def test_query_with_connection_url(postgres: PostgresContainer):
    connection_url = to_connection_url(postgres)
    response = client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0][0] == "2024-01-01 23:59:59.000000"
    assert result["dtypes"] is not None


def test_query_with_limit(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
    response = client.post(
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

    response = client.post(
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


def test_query_with_invalid_manifest_str(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
    response = client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": "xxx",
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 422
    assert response.text == "Invalid padding"


def test_query_without_manifest(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
    response = client.post(
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


def test_query_without_sql(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
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


def test_query_without_connection_info():
    response = client.post(
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


def test_query_with_dry_run(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
    response = client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 204


def test_query_with_dry_run_and_invalid_sql(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
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


def test_validate_with_unknown_rule(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
    response = client.post(
        url=f"{base_url}/validate/unknown_rule",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders", "columnName": "o_orderkey"},
        },
    )
    assert response.status_code == 422
    assert (
        response.text == f"The rule `unknown_rule` is not in the rules, rules: {rules}"
    )


def test_validate_rule_column_is_valid(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders", "columnName": "o_orderkey"},
        },
    )
    assert response.status_code == 204


def test_validate_rule_column_is_valid_with_invalid_parameters(
    postgres: PostgresContainer,
):
    connection_info = to_connection_info(postgres)
    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "X", "columnName": "o_orderkey"},
        },
    )
    assert response.status_code == 422

    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders", "columnName": "X"},
        },
    )
    assert response.status_code == 422


def test_validate_rule_column_is_valid_without_parameters(postgres: PostgresContainer):
    connection_info = to_connection_info(postgres)
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
    postgres: PostgresContainer,
):
    connection_info = to_connection_info(postgres)
    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders"},
        },
    )
    assert response.status_code == 422
    assert response.text == "Missing required parameter: `columnName`"

    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"columnName": "o_orderkey"},
        },
    )
    assert response.status_code == 422
    assert response.text == "Missing required parameter: `modelName`"


def test_dry_plan():
    response = client.post(
        url=f"{base_url}/dry-plan",
        json={
            "manifestStr": manifest_str,
            "sql": "SELECT o_orderkey, order_cust_key FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    assert response.text is not None


def to_connection_info(pg: PostgresContainer):
    return {
        "host": pg.get_container_host_ip(),
        "port": pg.get_exposed_port(pg.port),
        "user": pg.username,
        "password": pg.password,
        "database": pg.dbname,
    }


def to_connection_url(pg: PostgresContainer):
    info = to_connection_info(pg)
    return f"postgres://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"
