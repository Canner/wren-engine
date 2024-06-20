import base64

import orjson
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from testcontainers.mysql import MySqlContainer

from app.main import app

client = TestClient(app)


@pytest.fixture(scope="class")
def mysql(request) -> MySqlContainer:
    def file_path(path: str) -> str:
        import os

        return os.path.join(os.path.dirname(__file__), path)

    import pandas as pd
    import sqlalchemy

    mysql = MySqlContainer("mysql:8.0")
    mysql.start()
    connection_url = mysql.get_connection_url()
    engine = sqlalchemy.create_engine(connection_url)
    pd.read_parquet(file_path("../../resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    pd.read_parquet(file_path("../../resource/tpch/data/customer.parquet")).to_sql(
        "customer", engine, index=False
    )
    conn = engine.connect()
    conn.execute(text("ALTER TABLE orders ADD PRIMARY KEY(o_orderkey);"))
    conn.execute(text("ALTER TABLE customer Add PRIMARY KEY(c_custkey);"))
    conn.execute(
        text(
            "ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey);"
        )
    )
    conn.close()

    def stop_pg():
        mysql.stop()

    request.addfinalizer(stop_pg)

    return mysql


@pytest.mark.mysql
class TestMySql:
    manifest = {
        "catalog": "my_catalog",
        "schema": "my_schema",
        "models": [
            {
                "name": "Orders",
                "refSql": "select * from orders",
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
                "refSql": "select * from customer",
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
    def to_connection_info(mysql: MySqlContainer):
        return {
            "host": mysql.get_container_host_ip(),
            "port": mysql.get_exposed_port(mysql.port),
            "user": mysql.username,
            "password": mysql.password,
            "database": mysql.dbname,
        }

    @staticmethod
    def to_connection_url(mysql: MySqlContainer):
        info = TestMySql.to_connection_info(mysql)
        return f"mysql://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"

    def test_query(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
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
            "172799.49",
            820540800000,
            "1_370",
            1704153599000,
            1704153599000,
        ]
        assert result["dtypes"] == {
            "orderkey": "int32",
            "custkey": "int32",
            "orderstatus": "object",
            "totalprice": "object",
            "orderdate": "object",
            "order_cust_key": "object",
            "timestamp": "datetime64[ns]",
            "timestamptz": "datetime64[ns]",
        }

    @pytest.mark.skip(
        reason="ibis miss port in connection_url, wait fix PR in ibis repo"
    )
    def test_query_with_connection_url(self, mysql: MySqlContainer):
        connection_url = self.to_connection_url(mysql)
        response = client.post(
            url="/v2/ibis/mysql/query",
            json={
                "connectionInfo": {"connectionUrl": connection_url},
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == len(self.manifest["models"][0]["columns"])
        assert len(result["data"]) == 1
        assert result["data"][0][0] == 1
        assert result["dtypes"] is not None

    def test_query_with_column_dtypes(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
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
            "2024-01-01 23:59:59.000000",
        ]
        assert result["dtypes"] == {
            "orderkey": "int32",
            "custkey": "int32",
            "orderstatus": "object",
            "totalprice": "float64",
            "orderdate": "object",
            "order_cust_key": "object",
            "timestamp": "object",
            "timestamptz": "object",
        }

    def test_query_without_manifest(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/query",
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

    def test_query_without_sql(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/query",
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
            url="/v2/ibis/mysql/query",
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

    def test_query_with_dry_run(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 204

    def test_query_with_dry_run_and_invalid_sql(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": "SELECT * FROM X",
            },
        )
        assert response.status_code == 422
        assert response.text is not None

    def test_validate_with_unknown_rule(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/validate/unknown_rule",
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

    def test_validate_rule_column_is_valid(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 204

    def test_validate_rule_column_is_valid_with_invalid_parameters(
        self, mysql: MySqlContainer
    ):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "X", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 422

        response = client.post(
            url="/v2/ibis/mysql/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "X"},
            },
        )
        assert response.status_code == 422

    def test_validate_rule_column_is_valid_without_parameters(
        self, mysql: MySqlContainer
    ):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/validate/column_is_valid",
            json={"connectionInfo": connection_info, "manifestStr": self.manifest_str},
        )
        assert response.status_code == 422
        result = response.json()
        assert result["detail"][0] is not None
        assert result["detail"][0]["type"] == "missing"
        assert result["detail"][0]["loc"] == ["body", "parameters"]
        assert result["detail"][0]["msg"] == "Field required"

    def test_validate_rule_column_is_valid_without_one_parameter(
        self, mysql: MySqlContainer
    ):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `columnName`"

        response = client.post(
            url="/v2/ibis/mysql/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"columnName": "orderkey"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `modelName`"

    def test_metadata_list_tables(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/metadata/tables",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200

        result = response.json()[0]
        assert result["name"] is not None
        assert result["columns"] is not None
        assert result["primaryKey"] is not None
        assert result["description"] is not None
        assert result["properties"] is not None

    def test_metadata_list_constraints(self, mysql: MySqlContainer):
        connection_info = self.to_connection_info(mysql)
        response = client.post(
            url="/v2/ibis/mysql/metadata/constraints",
            json={"connectionInfo": connection_info},
        )
        assert response.status_code == 200

        result = response.json()[0]
        assert result["constraintName"] is not None
        assert result["constraintType"] is not None
        assert result["constraintTable"] is not None
        assert result["constraintColumn"] is not None
        assert result["constraintedTable"] is not None
        assert result["constraintedColumn"] is not None
