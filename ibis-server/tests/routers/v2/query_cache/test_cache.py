import base64
import os
import shutil

import orjson
import pandas as pd
import pytest
import sqlalchemy
from testcontainers.postgres import PostgresContainer

from app.query_cache import QueryCacheManager
from tests.conftest import file_path

pytestmark = pytest.mark.cache

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
    # Load sample data
    pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
        "customer", engine, index=False
    )
    request.addfinalizer(pg.stop)
    return pg


@pytest.fixture(scope="function")
def cache_dir():
    temp_dir = "/tmp/wren-engine-test"
    os.makedirs(temp_dir, exist_ok=True)
    yield temp_dir
    # Clean up after the test
    shutil.rmtree(temp_dir, ignore_errors=True)


async def test_query_with_cache(
    client, manifest_str, postgres: PostgresContainer, cache_dir, monkeypatch
):
    # Override the cache path to use our test directory
    monkeypatch.setattr(
        QueryCacheManager,
        "_get_cache_path",
        lambda self, key: f"{cache_dir}/{key}.cache",
    )

    connection_info = _to_connection_info(postgres)
    test_sql = 'SELECT * FROM "Orders" LIMIT 10'

    # First request - should miss cache
    response1 = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": test_sql,
        },
    )
    assert response1.status_code == 200
    result1 = response1.json()

    # Second request with same SQL - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": test_sql,
        },
    )
    assert response2.status_code == 200
    result2 = response2.json()

    # Verify results are identical
    assert result1["data"] == result2["data"]
    assert result1["columns"] == result2["columns"]
    assert result1["dtypes"] == result2["dtypes"]


def _to_connection_info(pg: PostgresContainer):
    return {
        "host": pg.get_container_host_ip(),
        "port": pg.get_exposed_port(pg.port),
        "user": pg.username,
        "password": pg.password,
        "database": pg.dbname,
    }
