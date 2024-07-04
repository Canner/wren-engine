import base64
import os

import clickhouse_connect
import orjson
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from testcontainers.clickhouse import ClickHouseContainer

from app.main import app

client = TestClient(app)


@pytest.fixture(scope="class")
def clickhouse(request) -> ClickHouseContainer:
    ch = ClickHouseContainer("clickhouse/clickhouse-server:head-alpine", port=8123)
    ch.start()
    client = clickhouse_connect.get_client(
        host=ch.get_container_host_ip(),
        port=int(ch.get_exposed_port(ch.port)),
        database=ch.dbname,
        user=ch.username,
        password=ch.password,
    )
    client.command("""
        CREATE TABLE orders (
            o_orderkey       Int32,
            o_custkey        Int32,
            o_orderstatus    String,
            o_totalprice     Decimal(15,2),
            o_orderdate      Date,
            o_orderpriority  String,
            o_clerk          String,
            o_shippriority   Int32,
            o_comment        String
        ) 
        ENGINE = MergeTree
        ORDER BY (o_orderkey)
    """)
    data_path = os.path.join(
        os.path.dirname(__file__), "../../resource/tpch/data/orders.parquet"
    )
    client.insert_df("orders", pd.read_parquet(data_path))
    client.command("""
        CREATE TABLE customer (
            c_custkey        Int32,
            c_name           String,
            c_address        String,
            c_nationkey      Int32,
            c_phone          String,
            c_acctbal        Decimal(15,2),
            c_mktsegment     String,
            c_comment        String
        ) 
        ENGINE = MergeTree
        ORDER BY (c_custkey)
    """)
    data_path = os.path.join(
        os.path.dirname(__file__), "../../resource/tpch/data/customer.parquet"
    )
    client.insert_df("customer", pd.read_parquet(data_path))
    request.addfinalizer(ch.stop)
    return ch


@pytest.mark.clickhouse
class TestClickHouse:
    base_url = "/v2/ibis/clickhouse"

    manifest = {
        "catalog": "my_catalog",
        "schema": "my_schema",
        "models": [
            {
                "name": "Orders",
                "refSql": "select * from test.orders",
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
                        "expression": "toDateTime64('2024-01-01T23:59:59', 9)",
                        "type": "timestamp",
                    },
                    {
                        "name": "timestamptz",
                        "expression": "toDateTime64('2024-01-01T23:59:59', 9, 'UTC')",
                        "type": "timestamp",
                    },
                    {
                        "name": "customer",
                        "type": "Customer",
                        "relationship": "OrdersCustomer",
                    },
                    {
                        "name": "customer_name",
                        "type": "varchar",
                        "isCalculated": True,
                        "expression": "customer.name",
                    },
                ],
                "primaryKey": "orderkey",
            },
            {
                "name": "Customer",
                "refSql": "select * from test.customer",
                "columns": [
                    {"name": "custkey", "expression": "c_custkey", "type": "integer"},
                    {"name": "name", "expression": "c_name", "type": "varchar"},
                    {
                        "name": "orders",
                        "type": "Orders",
                        "relationship": "OrdersCustomer",
                    },
                    {
                        "name": "totalprice",
                        "type": "float",
                        "isCalculated": True,
                        "expression": "sum(orders.totalprice)",
                    },
                ],
                "primaryKey": "custkey",
            },
        ],
        "relationships": [
            {
                "name": "OrdersCustomer",
                "models": ["Orders", "Customer"],
                "joinType": "MANY_TO_ONE",
                "condition": "Orders.custkey = Customer.custkey",
            },
        ],
    }

    manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")

    @staticmethod
    def to_connection_info(db: ClickHouseContainer):
        return {
            "host": db.get_container_host_ip(),
            "port": db.get_exposed_port(db.port),
            "user": db.username,
            "password": db.password,
            "database": db.dbname,
        }

    @staticmethod
    def to_connection_url(ch: ClickHouseContainer):
        info = TestClickHouse.to_connection_info(ch)
        return f"clickhouse://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"

    def test_query(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == 9
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
            "Customer#000000370",
        ]
        assert result["dtypes"] == {
            "orderkey": "int32",
            "custkey": "int32",
            "orderstatus": "object",
            "totalprice": "object",
            "orderdate": "object",
            "order_cust_key": "object",
            "timestamp": "datetime64[ns]",
            "timestamptz": "datetime64[ns, UTC]",
            "customer_name": "object",
        }

    def test_query_with_connection_url(self, clickhouse: ClickHouseContainer):
        connection_url = self.to_connection_url(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
            json={
                "connectionInfo": {"connectionUrl": connection_url},
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == 9
        assert len(result["data"]) == 1
        assert result["data"][0][0] == 1
        assert result["dtypes"] is not None

    def test_query_with_column_dtypes(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
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
        assert len(result["columns"]) == 9
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
            "Customer#000000370",
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
            "customer_name": "object",
        }

    def test_query_with_limit(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
            params={"limit": 1},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders"',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["data"]) == 1

        response = client.post(
            url=f"{self.base_url}/query",
            params={"limit": 1},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 10',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["data"]) == 1

    def test_query_join(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT name as customer_name FROM "Orders" join "Customer" on "Orders".custkey = "Customer".custkey WHERE custkey = 370 LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == 1
        assert len(result["data"]) == 1
        assert result["data"][0] == ["Customer#000000370"]
        assert result["dtypes"] == {
            "customer_name": "object",
        }

    def test_query_to_one_relationship(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT customer_name FROM "Orders" where custkey = 370 LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == 1
        assert len(result["data"]) == 1
        assert result["data"][0] == ["Customer#000000370"]
        assert result["dtypes"] == {
            "customer_name": "object",
        }

    def test_query_to_many_relationship(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT totalprice FROM "Customer" where custkey = 370 LIMIT 1',
            },
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["columns"]) == 1
        assert len(result["data"]) == 1
        assert result["data"][0] == [2860895.79]
        assert result["dtypes"] == {
            "totalprice": "object",
        }

    def test_query_without_manifest(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
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

    def test_query_without_sql(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
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
            url=f"{self.base_url}/query",
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

    def test_query_with_dry_run(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": 'SELECT * FROM "Orders" LIMIT 1',
            },
        )
        assert response.status_code == 204

    def test_query_with_dry_run_and_invalid_sql(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/query",
            params={"dryRun": True},
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "sql": "SELECT * FROM X",
            },
        )
        assert response.status_code == 422
        assert response.text is not None

    def test_validate_with_unknown_rule(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/validate/unknown_rule",
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

    def test_validate_rule_column_is_valid(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 204

    def test_validate_rule_column_is_valid_with_invalid_parameters(
        self, clickhouse: ClickHouseContainer
    ):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "X", "columnName": "orderkey"},
            },
        )
        assert response.status_code == 422

        response = client.post(
            url=f"{self.base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders", "columnName": "X"},
            },
        )
        assert response.status_code == 422

    def test_validate_rule_column_is_valid_without_parameters(
        self, clickhouse: ClickHouseContainer
    ):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/validate/column_is_valid",
            json={"connectionInfo": connection_info, "manifestStr": self.manifest_str},
        )
        assert response.status_code == 422
        result = response.json()
        assert result["detail"][0] is not None
        assert result["detail"][0]["type"] == "missing"
        assert result["detail"][0]["loc"] == ["body", "parameters"]
        assert result["detail"][0]["msg"] == "Field required"

    def test_validate_rule_column_is_valid_without_one_parameter(
        self, clickhouse: ClickHouseContainer
    ):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"modelName": "Orders"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `columnName`"

        response = client.post(
            url=f"{self.base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": self.manifest_str,
                "parameters": {"columnName": "orderkey"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `modelName`"

    def test_metadata_list_tables(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/metadata/tables",
            json={
                "connectionInfo": connection_info,
            },
        )
        assert response.status_code == 200

        result = response.json()[0]
        assert result["name"] is not None
        assert result["columns"] is not None
        assert result["primaryKey"] is not None
        assert result["description"] is not None
        assert result["properties"] is not None

    def test_metadata_list_constraints(self, clickhouse: ClickHouseContainer):
        connection_info = self.to_connection_info(clickhouse)
        response = client.post(
            url=f"{self.base_url}/metadata/constraints",
            json={
                "connectionInfo": connection_info,
            },
        )
        assert response.status_code == 200

        result = response.json()
        assert len(result) == 0
