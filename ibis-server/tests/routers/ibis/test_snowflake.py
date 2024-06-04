import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.mark.snowflake
class TestSnowflake:

    @pytest.fixture()
    def manifest_str(self) -> str:
        import base64
        import orjson

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
                        {"name": "custkey", "type": "integer", "expression": "O_CUSTKEY"}
                    ],
                    "primaryKey": "orderkey"
                },
                {
                    "name": "Customer",
                    "refSql": "select * from TPCH_SF1.CUSTOMER",
                    "columns": [
                        {"name": "custkey", "expression": "C_CUSTKEY", "type": "integer"},
                        {"name": "name", "expression": "C_NAME", "type": "varchar"}
                    ],
                    "primaryKey": "custkey"
                }
            ]
        }
        return base64.b64encode(orjson.dumps(manifest)).decode('utf-8')

    @staticmethod
    def get_connection_info():
        import os
        return {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "database": "SNOWFLAKE_SAMPLE_DATA",
            "schema": "TPCH_SF1"
        }

    def test_query(self, manifest_str: str):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/snowflake/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1'
            }
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result['columns']) == 2
        assert len(result['data']) == 1
        assert result['data'][0][0] is not None
        assert result['dtypes'] is not None

    def test_query_without_manifest(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/snowflake/query",
            json={
                "connectionInfo": connection_info,
                "sql": 'SELECT * FROM "Orders" LIMIT 1'
            }
        )
        assert response.status_code == 422
        result = response.json()
        assert result['detail'][0] is not None
        assert result['detail'][0]['type'] == 'missing'
        assert result['detail'][0]['loc'] == ['body', 'manifestStr']
        assert result['detail'][0]['msg'] == 'Field required'

    def test_query_without_sql(self, manifest_str: str):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/snowflake/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str
            }
        )
        assert response.status_code == 422
        result = response.json()
        assert result['detail'][0] is not None
        assert result['detail'][0]['type'] == 'missing'
        assert result['detail'][0]['loc'] == ['body', 'sql']
        assert result['detail'][0]['msg'] == 'Field required'

    def test_query_without_connection_info(self, manifest_str: str):
        response = client.post(
            url="/v2/ibis/snowflake/query",
            json={
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1'
            }
        )
        assert response.status_code == 422
        result = response.json()
        assert result['detail'][0] is not None
        assert result['detail'][0]['type'] == 'missing'
        assert result['detail'][0]['loc'] == ['body', 'connectionInfo']
        assert result['detail'][0]['msg'] == 'Field required'

    def test_query_with_dry_run(self, manifest_str: str):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/snowflake/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1'
            }
        )
        assert response.status_code == 200
        result = response.json()
        assert result['status'] == 'success'

    def test_query_with_dry_run_and_invalid_sql(self, manifest_str: str):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/snowflake/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM X'
            }
        )
        assert response.status_code == 200
        result = response.json()
        assert result['status'] == 'failure'
        assert result['message'] is not None
