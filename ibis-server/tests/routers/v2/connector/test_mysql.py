import base64

import orjson
import pandas as pd
import pymysql
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.mysql import MySqlContainer

from app.model import SSLMode
from app.model.error import ErrorCode
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
    mysql = MySqlContainer(image="mysql:8.0.40", dialect="pymysql").start()
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


@pytest.fixture(scope="module")
def mysql_ssl_off(request) -> MySqlContainer:
    mysql = MySqlContainer(image="mysql:8.0.40").with_command("--ssl=0").start()
    # We disable SSL for this container to test SSLMode.ENABLED.
    # However, Mysql use caching_sha2_password as default authentication plugin which requires the connection to use SSL.
    # MysqlDB used by ibis supports caching_sha2_password only with SSL. So, we need to change the authentication plugin to mysql_native_password.
    # Before changing the authentication plugin, we need to connect to the database using caching_sha2_password. That's why we use pymysql to connect to the database.
    # pymsql supports caching_sha2_password without SSL.
    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        passwd="test",
        port=int(mysql.get_exposed_port(mysql.port)),
    )

    cur = conn.cursor()
    cur.execute(
        "ALTER USER 'test'@'%' IDENTIFIED WITH mysql_native_password BY 'test';"
    )
    cur.execute("FLUSH PRIVILEGES;")
    conn.commit()
    conn.close()

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
        "2024-01-01 23:59:59.000000 +00:00",
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
        "timestamp": "timestamp[us, tz=UTC]",
        "timestamptz": "timestamp[us, tz=UTC]",
        "test_null_time": "timestamp[us, tz=UTC]",
        "bytea_column": "binary",
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


@pytest.mark.parametrize(
    "ssl_mode, error_code, expected_error",
    [
        (
            SSLMode.ENABLED,
            ErrorCode.GET_CONNECTION_ERROR,
            '(2026, "SSL connection error: SSL is required but the server doesn\'t support it")',
        ),
        (
            SSLMode.VERIFY_CA,
            ErrorCode.INVALID_CONNECTION_INFO,
            "SSL CA must be provided when SSL mode is VERIFY CA",
        ),
    ],
)
async def test_connection_invalid_ssl_mode(
    client, mysql_ssl_off: MySqlContainer, ssl_mode, error_code, expected_error
):
    connection_info = _to_connection_info(mysql_ssl_off)
    connection_info["sslMode"] = ssl_mode

    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )

    assert response.status_code == 422
    result = response.json()
    assert result["errorCode"] == error_code.name
    assert result["message"] == expected_error


async def test_connection_valid_ssl_mode(client, mysql_ssl_off: MySqlContainer):
    connection_info = _to_connection_info(mysql_ssl_off)
    connection_info["sslMode"] = SSLMode.DISABLED
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text == '"8.0.40"'


async def test_order_by_nulls_last(client, manifest_str, mysql: MySqlContainer):
    connection_info = _to_connection_info(mysql)
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
