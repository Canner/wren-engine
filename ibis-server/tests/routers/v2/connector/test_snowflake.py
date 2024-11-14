import base64
import os

import orjson
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.model.validator import rules

pytestmark = pytest.mark.snowflake

base_url = "/v2/connector/snowflake"

connection_info = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "database": "SNOWFLAKE_SAMPLE_DATA",
    "schema": "TPCH_SF1",
}

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "properties": {},
            "refSql": "select * from TPCH_SF1.ORDERS",
            "columns": [
                {"name": "orderkey", "expression": "O_ORDERKEY", "type": "integer"},
                {"name": "custkey", "expression": "O_CUSTKEY", "type": "integer"},
                {
                    "name": "orderstatus",
                    "expression": "O_ORDERSTATUS",
                    "type": "varchar",
                },
                {
                    "name": "totalprice",
                    "expression": "O_TOTALPRICE",
                    "type": "float",
                },
                {"name": "orderdate", "expression": "O_ORDERDATE", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "concat(O_ORDERKEY, '_', O_CUSTKEY)",
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
            ],
            "primaryKey": "orderkey",
        },
    ],
}


@pytest.fixture
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


with TestClient(app) as client:

    def test_query(manifest_str):
        response = client.post(
            url=f"{base_url}/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" ORDER BY "orderkey" LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == len(manifest["models"][0]["columns"])
        assert len(result["data"]) == 1
        assert result["data"][0] == [
            1,
            36901,
            "O",
            "173665.47",
            "1996-01-02",
            "1_36901",
            "2024-01-01 23:59:59.000000",
            "2024-01-01 23:59:59.000000 UTC",
            None,
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
        }

    def test_query_without_manifest():
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

    def test_query_without_sql(manifest_str):
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

    def test_query_with_dry_run(manifest_str):
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

    def test_query_with_dry_run_and_invalid_sql(manifest_str):
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

    def test_validate_with_unknown_rule(manifest_str):
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

    def test_validate_rule_column_is_valid(manifest_str):
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 204

    def test_validate_rule_column_is_valid_with_invalid_parameters(manifest_str):
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

    def test_validate_rule_column_is_valid_without_parameters(manifest_str):
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

    def test_validate_rule_column_is_valid_without_one_parameter(manifest_str):
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

    def test_metadata_list_tables():
        response = client.post(
            url=f"{base_url}/metadata/tables",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200
        tables = response.json()
        assert len(tables) == 8
        table = next(filter(lambda t: t["name"] == "TPCH_SF1.ORDERS", tables))
        assert table["name"] == "TPCH_SF1.ORDERS"
        assert table["primaryKey"] is not None
        assert table["description"] == "Orders data as defined by TPC-H"
        assert table["properties"] == {
            "catalog": "SNOWFLAKE_SAMPLE_DATA",
            "schema": "TPCH_SF1",
            "table": "ORDERS",
        }
        assert len(table["columns"]) == 9
        column = next(filter(lambda c: c["name"] == "O_COMMENT", table["columns"]))
        assert column == {
            "name": "O_COMMENT",
            "nestedColumns": None,
            "type": "TEXT",
            "notNull": True,
            "description": None,
            "properties": None,
        }

    def test_metadata_list_constraints():
        response = client.post(
            url=f"{base_url}/metadata/constraints",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200

        result = response.json()
        assert len(result) == 0

    def test_metadata_get_version():
        response = client.post(
            url=f"{base_url}/metadata/version",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200
        assert response.text is not None
