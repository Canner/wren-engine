import base64

import orjson
import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.mark.snowflake
class TestSnowflake:
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
                ],
                "primaryKey": "orderkey",
            },
            {
                "name": "Customer",
                "refSql": "select * from TPCH_SF1.CUSTOMER",
                "columns": [
                    {"name": "custkey", "expression": "C_CUSTKEY", "type": "integer"},
                    {"name": "name", "expression": "C_NAME", "type": "varchar"},
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
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "database": "SNOWFLAKE_SAMPLE_DATA",
            "schema": "TPCH_SF1",
        }

    def test_query(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/snowflake/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" ORDER BY "orderkey" LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == len(self.manifest["models"][0]["columns"])
        assert len(result["data"]) == 1
        assert result["data"][0] == [
            1,
            36901,
            "O",
            173665.47,
            820540800000,
            "1_36901",
            1704153599000,
            1704153599000,
        ]
        assert result["dtypes"] == {
            "orderkey": "int64",
            "custkey": "int64",
            "orderstatus": "object",
            "totalprice": "object",
            "orderdate": "object",
            "order_cust_key": "object",
            "timestamp": "datetime64[ns]",
            "timestamptz": "datetime64[ns, UTC]",
        }

    def test_query_with_column_dtypes(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/snowflake/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" ORDER BY "orderkey" LIMIT 1',
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
            36901,
            "O",
            173665.47,
            "1996-01-02 00:00:00.000000",
            "1_36901",
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
            url="/v2/ibis/snowflake/query",
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
            url="/v2/ibis/snowflake/query",
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
            url="/v2/ibis/snowflake/query",
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
            url="/v2/ibis/snowflake/query",
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
            url="/v2/ibis/snowflake/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": "SELECT * FROM X",
            },
        )
        assert response.status_code == 422
        assert response.text is not None
