import base64

import orjson
import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.mark.bigquery
class TestBigquery:
    manifest = {
        "catalog": "my_catalog",
        "schema": "my_schema",
        "models": [
            {
                "name": "Orders",
                "refSql": "select * from tpch_tiny.orders",
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
                "refSql": "select * from tpch_tiny.customer",
                "columns": [
                    {"name": "custkey", "expression": "c_custkey", "type": "integer"},
                    {"name": "name", "expression": "c_name", "type": "varchar"},
                ],
                "primaryKey": "custkey",
            },
        ],
    }

    manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")

    @staticmethod
    def get_connection_info():
        import os

        return {
            "project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
            "dataset_id": "tpch_tiny",
            "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
        }

    def test_query(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == len(self.manifest["models"][0]["columns"])
        assert len(result["data"]) == 1
        assert result["data"][0] == [
            1,
            370,
            "O",
            172799.49,
            820540800000,
            "1_370",
            1704153599000,
            1704153599000,
        ]
        assert result["dtypes"] == {
            "orderkey": "int64",
            "custkey": "int64",
            "orderstatus": "object",
            "totalprice": "float64",
            "orderdate": "object",
            "order_cust_key": "object",
            "timestamp": "datetime64[ns]",
            "timestamptz": "datetime64[ns, UTC]",
        }

    def test_query_with_column_dtypes(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
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
        assert len(result["columns"]) == len(self.manifest["models"][0]["columns"])
        assert len(result["data"]) == 1
        assert result["data"][0] == [
            1,
            370,
            "O",
            172799.49,
            "1996-01-02 00:00:00.000000",
            "1_370",
            "2024-01-01 23:59:59.000000",
            "2024-01-01 23:59:59.000000 UTC",
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
        }

    def test_query_without_manifest(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/query",
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

    def test_query_without_sql(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/query",
            json={"connectionInfo": connection_info, "manifestStr": self.manifest_str},
        )
        assert response.status_code == 422
        result = response.json()
        assert result["detail"][0] is not None
        assert result["detail"][0]["type"] == "missing"
        assert result["detail"][0]["loc"] == ["body", "sql"]
        assert result["detail"][0]["msg"] == "Field required"

    def test_query_without_connection_info(self):
        response = client.post(
            url="/v2/connector/bigquery/query",
            json={
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 422
        result = response.json()
        assert result["detail"][0] is not None
        assert result["detail"][0]["type"] == "missing"
        assert result["detail"][0]["loc"] == ["body", "connectionInfo"]
        assert result["detail"][0]["msg"] == "Field required"

    def test_query_with_dry_run(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 204

    def test_query_with_dry_run_and_invalid_sql(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": "SELECT * FROM X",
            },
        )
        assert response.status_code == 422
        assert response.text is not None

    def test_validate_with_unknown_rule(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/validate/unknown_rule",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 422
        assert (
            response.text
            == "The rule `unknown_rule` is not in the rules, rules: ['column_is_valid']"
        )

    def test_validate_rule_column_is_valid(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 204

    def test_validate_rule_column_is_valid_with_invalid_parameters(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "X", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 422

        response = client.post(
            url="/v2/connector/bigquery/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "X"},
            },
        )
        assert response.status_code == 422

    def test_validate_rule_column_is_valid_without_parameters(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/validate/column_is_valid",
            json={"connectionInfo": connection_info, "manifestStr": self.manifest_str},
        )
        assert response.status_code == 422
        result = response.json()
        assert result["detail"][0] is not None
        assert result["detail"][0]["type"] == "missing"
        assert result["detail"][0]["loc"] == ["body", "parameters"]
        assert result["detail"][0]["msg"] == "Field required"

    def test_validate_rule_column_is_valid_without_one_parameter(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `columnName`"

        response = client.post(
            url="/v2/connector/bigquery/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"columnName": "orderkey"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `modelName`"

    def test_metadata_list_tables(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/metadata/tables",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200

    def test_metadata_list_constraints(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/connector/bigquery/metadata/constraints",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200
