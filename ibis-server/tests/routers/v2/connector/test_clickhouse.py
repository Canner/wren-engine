import base64

import clickhouse_connect
import orjson
import pandas as pd
import pytest
from testcontainers.clickhouse import ClickHouseContainer

from app.model.data_source import X_WREN_DB_STATEMENT_TIMEOUT
from app.model.error import ErrorCode
from tests.conftest import file_path

pytestmark = pytest.mark.clickhouse

base_url = "/v2/connector/clickhouse"

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
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
                    "expression": "concat(o_orderkey, '_', o_custkey)",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "toDateTime64('2024-01-01T23:59:59', 9)",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "toDateTime64('2024-01-01T23:59:59', 9, 'UTC')",
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "toDateTime64(NULL, 9)",
                    "type": "timestamp",
                },
                {
                    "name": "bytea_column",
                    "expression": "cast('abc' as bytea)",
                    "type": "bytea",
                },
                {
                    "name": "customer",
                    "type": "Customer",
                    "relationship": "OrdersCustomer",
                },
                {
                    "name": "customer_name",
                    "type": "varchar",
                    "isCalculated": True,
                    "expression": "customer.name",
                },
            ],
            "primaryKey": "orderkey",
        },
        {
            "name": "Customer",
            "refSql": "select * from test.customer",
            "columns": [
                {"name": "custkey", "expression": "c_custkey", "type": "integer"},
                {"name": "name", "expression": "c_name", "type": "varchar"},
                {
                    "name": "orders",
                    "type": "Orders",
                    "relationship": "OrdersCustomer",
                },
                {
                    "name": "totalprice",
                    "type": "float",
                    "isCalculated": True,
                    "expression": "sum(orders.totalprice)",
                },
            ],
            "primaryKey": "custkey",
        },
    ],
    "relationships": [
        {
            "name": "OrdersCustomer",
            "models": ["Orders", "Customer"],
            "joinType": "MANY_TO_ONE",
            "condition": "Orders.custkey = Customer.custkey",
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def clickhouse(request) -> ClickHouseContainer:
    ch = ClickHouseContainer("clickhouse/clickhouse-server:head-alpine", port=8123)
    ch.start()
    client = clickhouse_connect.get_client(
        host=ch.get_container_host_ip(),
        port=int(ch.get_exposed_port(ch.port)),
        database=ch.dbname,
        user=ch.username,
        password=ch.password,
    )
    client.command("""
        CREATE TABLE orders (
            o_orderkey       Int32,
            o_custkey        Int32,
            o_orderstatus    String,
            o_totalprice     Decimal(15,2),
            o_orderdate      Date32,
            o_orderpriority  String,
            o_clerk          String,
            o_shippriority   Int32,
            o_comment        String COMMENT 'This is a comment'
        ) 
        ENGINE = MergeTree
        ORDER BY (o_orderkey)
        COMMENT 'This is a table comment'
    """)
    client.insert_df(
        "orders", pd.read_parquet(file_path("resource/tpch/data/orders.parquet"))
    )
    client.command("""
        CREATE TABLE customer (
            c_custkey        Int32,
            c_name           String,
            c_address        String,
            c_nationkey      Int32,
            c_phone          String,
            c_acctbal        Decimal(15,2),
            c_mktsegment     String,
            c_comment        String
        ) 
        ENGINE = MergeTree
        ORDER BY (c_custkey)
    """)
    client.insert_df(
        "customer", pd.read_parquet(file_path("resource/tpch/data/customer.parquet"))
    )
    request.addfinalizer(ch.stop)
    return ch


async def test_query(client, manifest_str, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
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
    assert len(result["columns"]) == 10
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
        "abc",  # Clickhouse does not support bytea, so it is returned as string
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "string",
        "totalprice": "decimal128(38, 9)",
        "orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[ns]",
        "timestamptz": "timestamp[ns, tz=UTC]",
        "test_null_time": "string",
        "bytea_column": "string",
    }


async def test_query_with_connection_url(
    client, manifest_str, clickhouse: ClickHouseContainer
):
    connection_url = _to_connection_url(clickhouse)
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
    assert len(result["columns"]) == 10
    assert len(result["data"]) == 1
    assert result["data"][0][0] == 1
    assert result["dtypes"] is not None


async def test_query_with_limit(client, manifest_str, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
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


async def test_query_join(client, manifest_str, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT name as customer_name FROM "Orders" join "Customer" on "Orders".custkey = "Customer".custkey WHERE custkey = 370 LIMIT 1',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1
    assert result["data"][0] == ["Customer#000000370"]
    assert result["dtypes"] == {
        "customer_name": "string",
    }


async def test_query_to_one_relationship(
    client, manifest_str, clickhouse: ClickHouseContainer
):
    connection_info = _to_connection_info(clickhouse)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT customer_name FROM "Orders" where custkey = 370 LIMIT 1',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1
    assert result["data"][0] == ["Customer#000000370"]
    assert result["dtypes"] == {
        "customer_name": "string",
    }


async def test_query_to_many_relationship(
    client, manifest_str, clickhouse: ClickHouseContainer
):
    connection_info = _to_connection_info(clickhouse)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT totalprice FROM "Customer" where custkey = 370 LIMIT 1',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1
    assert result["data"][0] == ["2860895.79"]
    assert result["dtypes"] == {
        "totalprice": "decimal128(38, 9)",
    }


async def test_query_alias_join(client, manifest_str, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
    # ClickHouse does not support alias join
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT orderstatus FROM ("Orders" o JOIN "Customer" c ON o.custkey = c.custkey) j1 LIMIT 1',
        },
    )

    assert response.status_code == 422
    assert response.json()["errorCode"] == ErrorCode.INVALID_SQL.name


async def test_query_without_manifest(client, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
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


async def test_query_without_sql(client, manifest_str, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
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


async def test_query_with_dry_run(
    client, manifest_str, clickhouse: ClickHouseContainer
):
    connection_info = _to_connection_info(clickhouse)
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
    client, manifest_str, clickhouse: ClickHouseContainer
):
    connection_info = _to_connection_info(clickhouse)
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


async def test_metadata_list_tables(client, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200

    results = response.json()
    orders = next((x for x in results if x["name"] == "test.orders"), None)
    assert orders is not None
    assert orders["name"] == "test.orders"
    assert orders["primaryKey"] is not None
    assert orders["description"] == "This is a table comment"
    assert orders["properties"] == {
        "catalog": None,
        "schema": "test",
        "table": "orders",
        "path": None,
    }
    assert len(orders["columns"]) == 9
    assert orders["columns"][8] == {
        "name": "o_comment",
        "nestedColumns": None,
        "type": "VARCHAR",
        "notNull": False,
        "description": "This is a comment",
        "properties": None,
    }


async def test_metadata_list_constraints(client, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200

    result = response.json()
    assert len(result) == 0


async def test_metadata_db_version(client, clickhouse: ClickHouseContainer):
    connection_info = _to_connection_info(clickhouse)
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert response.text is not None


async def test_connection_timeout(
    client, manifest_str, clickhouse: ClickHouseContainer
):
    connection_info = _to_connection_info(clickhouse)
    # Set a very short timeout to force a timeout error
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT sleep(3)",  # This will take longer than the default timeout
        },
        headers={X_WREN_DB_STATEMENT_TIMEOUT: "1"},  # Set timeout to 1 second
    )
    assert response.status_code == 504  # Gateway Timeout
    assert "Query was cancelled:" in response.text

    connection_info = _to_connection_url(clickhouse)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_info},
            "manifestStr": manifest_str,
            "sql": "SELECT sleep(3)",  # This will take longer than the default timeout
        },
        headers={X_WREN_DB_STATEMENT_TIMEOUT: "1"},  # Set timeout to 1 second
    )
    assert response.status_code == 504  # Gateway Timeout
    assert "Query was cancelled:" in response.text


def _to_connection_info(db: ClickHouseContainer):
    return {
        "host": db.get_container_host_ip(),
        "port": db.get_exposed_port(db.port),
        "user": db.username,
        "password": db.password,
        "database": db.dbname,
    }


def _to_connection_url(ch: ClickHouseContainer):
    info = _to_connection_info(ch)
    return f"clickhouse://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"
