import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.mark.bigquery
class TestBigquery:

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
                    "refSql": "select * from tpch_tiny.orders",
                    "columns": [
                        {"name": "orderkey", "expression": "o_orderkey", "type": "integer"},
                        {"name": "custkey", "type": "integer", "expression": "o_custkey"}
                    ],
                    "primaryKey": "orderkey"
                },
                {
                    "name": "Customer",
                    "refSql": "select * from tpch_tiny.customer",
                    "columns": [
                        {"name": "custkey", "expression": "c_custkey", "type": "integer"},
                        {"name": "name", "expression": "c_name", "type": "varchar"}
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
            "project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
            "dataset_id": "tpch_sf1",
            "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON")
        }

    def test_bigquery(self, manifest_str: str):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/bigquery/query",
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

    def test_no_manifest(self):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/bigquery/query",
            json={
                "connectionInfo": connection_info,
                "sql": "SELECT * FROM Orders LIMIT 1"
            }
        )
        assert response.status_code == 422
        result = response.json()
        assert result['detail'][0] is not None
        assert result['detail'][0]['type'] == 'missing'
        assert result['detail'][0]['loc'] == ['body', 'manifestStr']
        assert result['detail'][0]['msg'] == 'Field required'

    def test_no_sql(self, manifest_str: str):
        connection_info = self.get_connection_info()
        response = client.post(
            url="/v2/ibis/bigquery/query",
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

    def test_no_connection_info(self, manifest_str: str):
        response = client.post(
            url="/v2/ibis/bigquery/query",
            json={
                "manifestStr": manifest_str,
                "sql": "SELECT * FROM Orders LIMIT 1"
            }
        )
        assert response.status_code == 422
        result = response.json()
        assert result['detail'][0] is not None
        assert result['detail'][0]['type'] == 'missing'
        assert result['detail'][0]['loc'] == ['body', 'connectionInfo']
        assert result['detail'][0]['msg'] == 'Field required'
