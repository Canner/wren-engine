import base64

import orjson
import pandas as pd
import pytest
import sqlalchemy
from fastapi.testclient import TestClient
from sqlalchemy import text
from testcontainers.mysql import MySqlContainer

from app.main import app
from tests.confest import file_path

pytestmark = pytest.mark.mysql

client = TestClient(app)

base_url = "/v2/connector/mysql"

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "refSql": "select * from orders",
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
            ],
            "primaryKey": "orderkey",
        },
        {
            "name": "Customer",
            "refSql": "select * from customer",
            "columns": [
                {"name": "custkey", "expression": "c_custkey", "type": "integer"},
                {"name": "name", "expression": "c_name", "type": "varchar"},
            ],
            "primaryKey": "custkey",
        },
    ],
}

manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def mysql(request) -> MySqlContainer:
    mysql = MySqlContainer("mysql:8.0").start()
    connection_url = mysql.get_connection_url()
    engine = sqlalchemy.create_engine(connection_url)
    pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
        "customer", engine, index=False
    )
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE orders ADD PRIMARY KEY(o_orderkey);"))
        conn.execute(text("ALTER TABLE customer Add PRIMARY KEY(c_custkey);"))
        conn.execute(
            text(
                "ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey);"
            )
        )
    request.addfinalizer(mysql.stop)
    return mysql


def test_query(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
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
        820540800000,
        "1_370",
        1704153599000,
        1704153599000,
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "object",
        "totalprice": "object",
        "orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "datetime64[ns]",
        "timestamptz": "datetime64[ns]",
    }


def test_query_with_connection_url(mysql: MySqlContainer):
    connection_url = to_connection_url(mysql)
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


def test_query_with_column_dtypes(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
    response = client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
            "columnDtypes": {
                "totalprice": "float",
                "orderdate": "datetime64",
                "timestamp": "datetime64",
                "timestamptz": "datetime64",
            },
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
        "1996-01-02 00:00:00.000000",
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000",
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "object",
        "totalprice": "float64",
        "orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "object",
        "timestamptz": "object",
    }


def test_query_without_manifest(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
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


def test_query_without_sql(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
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
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "connectionInfo"]
    assert result["detail"][0]["msg"] == "Field required"


def test_query_with_dry_run(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
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


def test_query_with_dry_run_and_invalid_sql(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
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


def test_validate_with_unknown_rule(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
    response = client.post(
        url=f"{base_url}/validate/unknown_rule",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 422
    assert (
        response.text
        == "The rule `unknown_rule` is not in the rules, rules: ['column_is_valid']"
    )


def test_validate_rule_column_is_valid(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 204


def test_validate_rule_column_is_valid_with_invalid_parameters(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
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


def test_validate_rule_column_is_valid_without_parameters(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
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


def test_validate_rule_column_is_valid_without_one_parameter(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
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


def test_metadata_list_tables(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
    response = client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    result = response.json()[0]
    assert result["name"] is not None
    assert result["columns"] is not None
    assert result["primaryKey"] is not None
    assert result["description"] is not None
    assert result["properties"] is not None


def test_metadata_list_constraints(mysql: MySqlContainer):
    connection_info = to_connection_info(mysql)
    response = client.post(
        url=f"{base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    result = response.json()[0]
    assert result["constraintName"] is not None
    assert result["constraintType"] is not None
    assert result["constraintTable"] is not None
    assert result["constraintColumn"] is not None
    assert result["constraintedTable"] is not None
    assert result["constraintedColumn"] is not None


def to_connection_info(mysql: MySqlContainer):
    return {
        "host": mysql.get_container_host_ip(),
        "port": mysql.get_exposed_port(mysql.port),
        "user": mysql.username,
        "password": mysql.password,
        "database": mysql.dbname,
    }


def to_connection_url(mysql: MySqlContainer):
    info = to_connection_info(mysql)
    return f"mysql://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"
