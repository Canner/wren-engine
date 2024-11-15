import base64
import time

import orjson
import pytest
from fastapi.testclient import TestClient
from testcontainers.trino import TrinoContainer
from trino.dbapi import connect

from app.main import app
from app.model.validator import rules

pytestmark = pytest.mark.trino

base_url = "/v2/connector/trino"

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "refSql": "select * from tpch.tiny.orders",
            "columns": [
                {"name": "orderkey", "expression": "orderkey", "type": "integer"},
                {"name": "custkey", "expression": "custkey", "type": "integer"},
                {
                    "name": "orderstatus",
                    "expression": "orderstatus",
                    "type": "varchar",
                },
                {
                    "name": "totalprice",
                    "expression": "totalprice",
                    "type": "float",
                },
                {"name": "orderdate", "expression": "orderdate", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "concat(cast(orderkey as varchar), '_', cast(custkey as varchar))",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "TIMESTAMP '2024-01-01 23:59:59'",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "TIMESTAMP '2024-01-01 23:59:59 UTC'",
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
    ],
}


@pytest.fixture(scope="module")
def trino(request) -> TrinoContainer:
    db = TrinoContainer().start()

    # To avoid `TrinoQueryError(type=INTERNAL_ERROR, name=GENERIC_INTERNAL_ERROR, message="nodes is empty")`
    time.sleep(5)

    conn = connect(
        host=db.get_container_host_ip(),
        port=db.get_exposed_port(db.port),
        user="test",
    )
    cur = conn.cursor()
    cur.execute("CREATE TABLE memory.default.orders AS SELECT * from tpch.tiny.orders")
    cur.execute("COMMENT ON TABLE memory.default.orders IS 'This is a table comment'")
    cur.execute(
        "COMMENT ON COLUMN memory.default.orders.comment IS 'This is a comment'"
    )

    request.addfinalizer(db.stop)
    return db


@pytest.fixture
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


with TestClient(app) as client:

    def test_query(manifest_str, trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
        response = client.post(
            url=f"{base_url}/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
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
            "1996-01-02",
            "1_370",
            "2024-01-01 23:59:59.000000",
            "2024-01-01 23:59:59.000000",
            None,
            "616263",
        ]
        assert result["dtypes"] == {
            "orderkey": "int64",
            "custkey": "int64",
            "orderstatus": "object",
            "totalprice": "float64",
            "orderdate": "object",
            "order_cust_key": "object",
            "timestamp": "object",
            "timestamptz": "object",
            "test_null_time": "datetime64[ns]",
            "bytea_column": "object",
        }

    def test_query_with_connection_url(manifest_str, trino: TrinoContainer):
        connection_url = _to_connection_url(trino)
        response = client.post(
            url=f"{base_url}/query",
            json={
                "connectionInfo": {"connectionUrl": connection_url},
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == len(manifest["models"][0]["columns"])
        assert len(result["data"]) == 1
        assert result["data"][0][0] == 1
        assert result["dtypes"] is not None

    def test_query_with_limit(manifest_str, trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
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

    def test_query_without_manifest(trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
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

    def test_query_without_sql(manifest_str, trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
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

    def test_query_with_dry_run(manifest_str, trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
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

    def test_query_with_dry_run_and_invalid_sql(manifest_str, trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
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

    def test_validate_with_unknown_rule(manifest_str, trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
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

    def test_validate_rule_column_is_valid(manifest_str, trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
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
        manifest_str, trino: TrinoContainer
    ):
        connection_info = _to_connection_info(trino)
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
        manifest_str, trino: TrinoContainer
    ):
        connection_info = _to_connection_info(trino)
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
        manifest_str, trino: TrinoContainer
    ):
        connection_info = _to_connection_info(trino)
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

    def test_metadata_list_tables(trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
        response = client.post(
            url=f"{base_url}/metadata/tables",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200
        tables = response.json()
        assert len(tables) == 8
        table = next(filter(lambda t: t["name"] == "tpch.tiny.customer", tables))
        assert len(table["columns"]) == 8

        connection_info = {
            "host": trino.get_container_host_ip(),
            "port": trino.get_exposed_port(trino.port),
            "catalog": "memory",
            "schema": "default",
            "user": "test",
        }
        response = client.post(
            url=f"{base_url}/metadata/tables",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200
        tables = response.json()
        assert len(tables) == 1
        table = next(filter(lambda t: t["name"] == "memory.default.orders", tables))
        assert table["name"] == "memory.default.orders"
        assert table["primaryKey"] is not None
        assert table["description"] == "This is a table comment"
        assert table["properties"] == {
            "catalog": "memory",
            "schema": "default",
            "table": "orders",
        }
        assert len(table["columns"]) == 9
        column = next(filter(lambda c: c["name"] == "comment", table["columns"]))
        assert column == {
            "name": "comment",
            "nestedColumns": None,
            "type": "VARCHAR",
            "notNull": False,
            "description": "This is a comment",
            "properties": None,
        }

    def test_metadata_list_constraints(trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
        response = client.post(
            url=f"{base_url}/metadata/constraints",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200

        result = response.json()
        assert len(result) == 0

    def test_metadata_db_version(trino: TrinoContainer):
        connection_info = _to_connection_info(trino)
        response = client.post(
            url=f"{base_url}/metadata/version",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200
        assert response.text is not None

    def _to_connection_info(trino: TrinoContainer):
        return {
            "host": trino.get_container_host_ip(),
            "port": trino.get_exposed_port(trino.port),
            "catalog": "tpch",
            "schema": "tiny",
            "user": "test",
        }

    def _to_connection_url(trino: TrinoContainer):
        info = _to_connection_info(trino)
        return f"trino://{info['user']}@{info['host']}:{info['port']}/{info['catalog']}/{info['schema']}"
