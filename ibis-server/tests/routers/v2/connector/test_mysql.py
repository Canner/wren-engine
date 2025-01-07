import base64

import orjson
import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.mysql import MySqlContainer

from app.model.validator import rules
from tests.conftest import file_path

pytestmark = pytest.mark.mysql

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
            "refSql": "select * from customer",
            "columns": [
                {"name": "custkey", "expression": "c_custkey", "type": "integer"},
                {"name": "name", "expression": "c_name", "type": "varchar"},
            ],
            "primaryKey": "custkey",
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def mysql(request) -> MySqlContainer:
    mysql = MySqlContainer("mysql:8.0.40").start()
    connection_url = mysql.get_connection_url()
    engine = sqlalchemy.create_engine(connection_url)
    pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
        "customer", engine, index=False
    )
    with engine.begin() as conn:
        conn.execute(
            text("""
            ALTER TABLE orders
                ADD PRIMARY KEY (o_orderkey),
                COMMENT = 'This is a table comment',
                MODIFY COLUMN o_comment VARCHAR(255) COMMENT 'This is a comment';
            """)
        )
        conn.execute(text("ALTER TABLE customer Add PRIMARY KEY(c_custkey);"))
        conn.execute(
            text(
                "ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey);"
            )
        )
    request.addfinalizer(mysql.stop)
    return mysql


async def test_query(client, manifest_str, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
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
        "2024-01-01 23:59:59.000000",
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


async def test_query_with_connection_url(client, manifest_str, mysql: MySqlContainer):
    connection_url = _to_connection_url(mysql)
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


async def test_query_without_manifest(client, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
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


async def test_query_without_sql(client, manifest_str, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
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


async def test_query_with_dry_run(client, manifest_str, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
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
    client, manifest_str, mysql: MySqlContainer
):
    connection_info = _to_connection_info(mysql)
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


async def test_validate_with_unknown_rule(client, manifest_str, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
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
    client, manifest_str, mysql: MySqlContainer
):
    connection_info = _to_connection_info(mysql)
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
    client, manifest_str, mysql: MySqlContainer
):
    connection_info = _to_connection_info(mysql)
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
    client, manifest_str, mysql: MySqlContainer
):
    connection_info = _to_connection_info(mysql)
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
    client, manifest_str, mysql: MySqlContainer
):
    connection_info = _to_connection_info(mysql)
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


async def test_metadata_list_tables(client, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    result = next(filter(lambda x: x["name"] == "test.orders", response.json()))
    assert result["name"] == "test.orders"
    assert result["primaryKey"] is not None
    assert result["description"] == "This is a table comment"
    assert result["properties"] == {
        "catalog": "",
        "schema": "test",
        "table": "orders",
        "path": None,
    }
    assert len(result["columns"]) == 9
    o_comment_column = next(
        filter(lambda x: x["name"] == "o_comment", result["columns"])
    )
    assert o_comment_column == {
        "name": "o_comment",
        "nestedColumns": None,
        "type": "VARCHAR",
        "notNull": False,
        "description": "This is a comment",
        "properties": None,
    }


async def test_metadata_list_constraints(client, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
    response = await client.post(
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


async def test_metadata_db_version(client, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text == '"8.0.40"'


def _to_connection_info(mysql: MySqlContainer):
    return {
        "host": mysql.get_container_host_ip(),
        "port": mysql.get_exposed_port(mysql.port),
        "user": mysql.username,
        "password": mysql.password,
        "database": mysql.dbname,
    }


def _to_connection_url(mysql: MySqlContainer):
    info = _to_connection_info(mysql)
    return f"mysql://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"
