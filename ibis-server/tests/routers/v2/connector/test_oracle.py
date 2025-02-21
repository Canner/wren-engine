import base64

import orjson
import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.oracle import OracleDbContainer

from tests.conftest import file_path

pytestmark = pytest.mark.oracle

base_url = "/v2/connector/oracle"

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "schema": "SYSTEM",
                "table": "ORDERS",
            },
            "columns": [
                {"name": "orderkey", "expression": "O_ORDERKEY", "type": "number"},
                {"name": "custkey", "expression": "O_CUSTKEY", "type": "number"},
                {
                    "name": "orderstatus",
                    "expression": "O_ORDERSTATUS",
                    "type": "varchar2",
                },
                {
                    "name": "totalprice",
                    "expression": "O_TOTALPRICE",
                    "type": "number",
                },
                {"name": "orderdate", "expression": "O_ORDERDATE", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "O_ORDERKEY || '_' || O_CUSTKEY",
                    "type": "varchar2",
                },
                {
                    "name": "timestamp",
                    "expression": "CAST('2024-01-01 23:59:59' AS TIMESTAMP)",
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "CAST(NULL AS TIMESTAMP)",
                    "type": "timestamp",
                },
                {
                    "name": "blob_column",
                    "expression": "UTL_RAW.CAST_TO_RAW('abc')",
                    "type": "blob",
                },
            ],
            "primaryKey": "orderkey",
        }
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def oracle(request) -> OracleDbContainer:
    oracle = OracleDbContainer(
        "gvenzl/oracle-free:23.6-slim-faststart", oracle_password="Oracle123"
    )

    oracle.start()

    host = oracle.get_container_host_ip()
    port = oracle.get_exposed_port(1521)
    connection_url = (
        f"oracle+oracledb://SYSTEM:Oracle123@{host}:{port}/?service_name=FREEPDB1"
    )
    engine = sqlalchemy.create_engine(connection_url, echo=True)

    with engine.begin() as conn:
        pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
            "orders", engine, index=False
        )
        pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
            "customer", engine, index=False
        )
        # Add table and column comments
        conn.execute(text("COMMENT ON TABLE orders IS 'This is a table comment'"))
        conn.execute(text("COMMENT ON COLUMN orders.o_comment IS 'This is a comment'"))

    request.addfinalizer(oracle.stop)
    return oracle


async def test_query_with_connection_url(
    client, manifest_str, oracle: OracleDbContainer
):
    connection_url = _to_connection_url(oracle)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM SYSTEM.ORDERS LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0][0] == 1
    assert result["dtypes"] is not None


def _to_connection_info(oracle: OracleDbContainer):
    return {
        "host": oracle.get_container_host_ip(),
        "port": oracle.get_exposed_port(oracle.port),
        "user": "SYSTEM",
        "password": "Oracle123",
        "service": "FREEPDB1",
    }


def _to_connection_url(oracle: OracleDbContainer):
    info = _to_connection_info(oracle)
    return f"oracle://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['service']}"
