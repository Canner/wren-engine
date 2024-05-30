import pytest
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from app.main import app

client = TestClient(app)


@pytest.fixture(scope="class")
def postgres(request) -> PostgresContainer:
    def file_path(path: str) -> str:
        import os
        return os.path.join(os.path.dirname(__file__), path)

    import sqlalchemy
    import pandas as pd

    pg = PostgresContainer("postgres:16-alpine")
    pg.start()
    psql_url = pg.get_connection_url()
    engine = sqlalchemy.create_engine(psql_url)
    pd.read_parquet(file_path("../resource/tpch/data/orders.parquet")).to_sql("orders", engine, index=False)
    pd.read_parquet(file_path("../resource/tpch/data/customer.parquet")).to_sql("customer", engine, index=False)

    def stop_pg():
        pg.stop()

    request.addfinalizer(stop_pg)

    return pg


@pytest.mark.postgres
class TestPostgres:

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
                    "refSql": "select * from public.orders",
                    "columns": [
                        {"name": "orderkey", "expression": "o_orderkey", "type": "integer"},
                        {"name": "custkey", "type": "integer", "expression": "o_custkey"}
                    ],
                    "primaryKey": "orderkey"
                },
                {
                    "name": "Customer",
                    "refSql": "select * from public.customer",
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
    def to_connection_info(pg: PostgresContainer):
        return {
            "host": pg.get_container_host_ip(),
            "port": pg.get_exposed_port(pg.port),
            "user": pg.username,
            "password": pg.password,
            "database": pg.dbname
        }

    @staticmethod
    def to_connection_url(pg: PostgresContainer):
        info = TestPostgres.to_connection_info(pg)
        return f"postgres://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"

    def test_postgres(self, postgres: PostgresContainer, manifest_str: str):
        connection_info = self.to_connection_info(postgres)
        response = client.post(
            url="/v2/ibis/postgres/query",
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
        assert result['data'][0][0] == 1
        assert result['dtypes'] is not None

    def test_postgres_with_connection_url(self, postgres: PostgresContainer, manifest_str: str):
        connection_url = self.to_connection_url(postgres)
        response = client.post(
            url="/v2/ibis/postgres/query",
            json={
                "connectionInfo": {
                    "connectionUrl": connection_url
                },
                "manifestStr": manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1'
            }
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result['columns']) == 2
        assert len(result['data']) == 1
        assert result['data'][0][0] == 1
        assert result['dtypes'] is not None

    def test_no_manifest(self, postgres: PostgresContainer):
        connection_info = self.to_connection_info(postgres)
        response = client.post(
            url="/v2/ibis/postgres/query",
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

    def test_no_sql(self, postgres: PostgresContainer, manifest_str: str):
        connection_info = self.to_connection_info(postgres)
        response = client.post(
            url="/v2/ibis/postgres/query",
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
            url="/v2/ibis/postgres/query",
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
