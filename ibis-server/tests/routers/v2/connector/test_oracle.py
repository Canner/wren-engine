import base64

import orjson
import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.oracle import OracleDbContainer

from app.model.validator import rules
from tests.conftest import file_path

pytestmark = pytest.mark.oracle

base_url = "/v2/connector/oracle"
oracle_password = "Oracle123"
oracle_user = "SYSTEM"
oracle_database = "FREEPDB1"

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
                    "type": "varchar",
                },
                {
                    "name": "totalprice",
                    "expression": "O_TOTALPRICE",
                    "type": "number",
                },
                {
                    "name": "orderdate",
                    "expression": "TRUNC(O_ORDERDATE)",
                    "type": "date",
                },
                {
                    "name": "order_cust_key",
                    "expression": "O_ORDERKEY || '_' || O_CUSTKEY",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "TO_TIMESTAMP('2024-01-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS')",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "TO_TIMESTAMP_TZ( '2024-01-01 23:59:59.000000 +00:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')",
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
        "gvenzl/oracle-free:23.6-slim-faststart", oracle_password=f"{oracle_password}"
    ).start()
    engine = sqlalchemy.create_engine(oracle.get_connection_url())
    with engine.begin() as conn:
        pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
            "orders", engine, index=False
        )
        pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
            "customer", engine, index=False
        )

        # Create a table with a large CLOB column
        large_text = "x" * (1024 * 1024 * 2)  # 2MB
        conn.execute(text("CREATE TABLE test_lob (id NUMBER, content CLOB)"))
        conn.execute(
            text("INSERT INTO test_lob VALUES (1, :content)"), {"content": large_text}
        )

        # Add table and column comments
        conn.execute(text("COMMENT ON TABLE orders IS 'This is a table comment'"))
        conn.execute(text("COMMENT ON COLUMN orders.o_comment IS 'This is a comment'"))

    return oracle


async def test_query(client, manifest_str, oracle: OracleDbContainer):
    connection_info = _to_connection_info(oracle)
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
        "1996-01-02 00:00:00.000000",
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 UTC",
        None,
        "616263",
    ]
    assert result["dtypes"] == {
        "orderkey": "int64",
        "custkey": "int64",
        "orderstatus": "object",
        "totalprice": "object",
        "orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "object",
        "timestamptz": "object",
        "test_null_time": "datetime64[ns]",
        "blob_column": "object",
    }


async def test_query_with_connection_url(
    client, manifest_str, oracle: OracleDbContainer
):
    connection_url = _to_connection_url(oracle)
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


async def test_query_without_manifest(client, oracle: OracleDbContainer):
    connection_info = _to_connection_info(oracle)
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


async def test_query_without_sql(client, manifest_str, oracle: OracleDbContainer):
    connection_info = _to_connection_info(oracle)
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "sql"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_without_connection_info(
    client, manifest_str, oracle: OracleDbContainer
):
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


async def test_query_with_dry_run(client, manifest_str, oracle: OracleDbContainer):
    connection_info = _to_connection_info(oracle)
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
    client, manifest_str, oracle: OracleDbContainer
):
    connection_info = _to_connection_info(oracle)
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "X"',
        },
    )
    assert response.status_code == 422
    assert (
        "ORA-00942" in response.text
    )  # Oracle ORA-00942 Error: Table or view does not exist


async def test_validate_with_unknown_rule(
    client, manifest_str, oracle: OracleDbContainer
):
    connection_info = _to_connection_info(oracle)
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
    client, manifest_str, oracle: OracleDbContainer
):
    connection_info = _to_connection_info(oracle)
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
    client, manifest_str, oracle: OracleDbContainer
):
    connection_info = _to_connection_info(oracle)
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


async def test_validate_rule_column_is_valid_without_parameters(
    client, manifest_str, oracle: OracleDbContainer
):
    connection_info = _to_connection_info(oracle)
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
    client, manifest_str, oracle: OracleDbContainer
):
    connection_info = _to_connection_info(oracle)
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


async def test_metadata_list_tables(client, oracle: OracleDbContainer):
    connection_info = _to_connection_info(oracle)
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200

    result = response.json()
    result = next(filter(lambda x: x["name"] == "SYSTEM.ORDERS", result))
    assert result is not None

    assert result["primaryKey"] is not None
    assert result["description"] == "This is a table comment"
    assert result["properties"] == {
        "catalog": "",
        "schema": "SYSTEM",
        "table": "ORDERS",
        "path": None,
    }
    assert len(result["columns"]) == 9
    assert result["columns"][8] == {
        "name": "O_COMMENT",
        "nestedColumns": None,
        "type": "TEXT",
        "notNull": False,
        "description": "This is a comment",
        "properties": None,
    }


async def test_metadata_list_constraints(client, oracle: OracleDbContainer):
    connection_info = _to_connection_info(oracle)
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    result = response.json()
    # oracale has a lot of constraints on the system tables
    assert len(result) > 0


async def test_metadata_db_version(client, oracle: OracleDbContainer):
    connection_info = _to_connection_info(oracle)
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={"connectionInfo": connection_info},
    )
    assert response.status_code == 200
    assert "23.0" in response.text


def _to_connection_info(oracle: OracleDbContainer):
    # We can't use oracle.user, oracle.password, oracle.dbname here
    # since these values are None at this point
    return {
        "host": oracle.get_container_host_ip(),
        "port": oracle.get_exposed_port(oracle.port),
        "user": f"{oracle_user}",
        "password": f"{oracle_password}",
        "database": f"{oracle_database}",
    }


def _to_connection_url(oracle: OracleDbContainer):
    info = _to_connection_info(oracle)
    return f"oracle://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"
