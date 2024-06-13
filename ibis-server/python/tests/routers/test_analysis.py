import base64

from fastapi.testclient import TestClient
from orjson import orjson

from python.wren_engine.main import app

client = TestClient(app)

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "customer",
            "refSql": "select * from main.customer",
            "columns": [
                {"name": "custkey", "expression": "custkey", "type": "integer"},
                {"name": "name", "expression": "name", "type": "varchar"},
                {"name": "address", "expression": "address", "type": "varchar"},
                {"name": "nationkey", "expression": "nationkey", "type": "integer"},
                {"name": "phone", "expression": "phone", "type": "varchar"},
                {"name": "acctbal", "expression": "acctbal", "type": "integer"},
                {"name": "mktsegment", "expression": "mktsegment", "type": "varchar"},
                {"name": "comment", "expression": "comment", "type": "varchar"},
            ],
            "primaryKey": "custkey",
        },
        {
            "name": "orders",
            "refSql": "select * from main.orders",
            "columns": [
                {"name": "orderkey", "expression": "orderkey", "type": "integer"},
                {"name": "custkey", "expression": "custkey", "type": "integer"},
                {"name": "orderstatus", "expression": "orderstatus", "type": "varchar"},
                {"name": "totalprice", "expression": "totalprice", "type": "integer"},
                {"name": "orderdate", "expression": "orderdate", "type": "date"},
                {
                    "name": "orderpriority",
                    "expression": "orderpriority",
                    "type": "varchar",
                },
                {"name": "clerk", "expression": "clerk", "type": "varchar"},
                {
                    "name": "shippriority",
                    "expression": "shippriority",
                    "type": "integer",
                },
                {"name": "comment", "expression": "comment", "type": "varchar"},
            ],
            "primaryKey": "orderkey",
        },
        {
            "name": "lineitem",
            "refSql": "select * from main.lineitem",
            "columns": [
                {"name": "orderkey", "expression": "orderkey", "type": "integer"},
                {"name": "partkey", "expression": "partkey", "type": "integer"},
                {"name": "suppkey", "expression": "suppkey", "type": "integer"},
                {"name": "linenumber", "expression": "linenumber", "type": "integer"},
                {"name": "quantity", "expression": "quantity", "type": "integer"},
                {
                    "name": "extendedprice",
                    "expression": "extendedprice",
                    "type": "integer",
                },
                {"name": "discount", "expression": "discount", "type": "integer"},
                {"name": "tax", "expression": "tax", "type": "integer"},
                {"name": "returnflag", "expression": "returnflag", "type": "varchar"},
                {"name": "linestatus", "expression": "linestatus", "type": "varchar"},
                {"name": "shipdate", "expression": "shipdate", "type": "date"},
                {"name": "commitdate", "expression": "commitdate", "type": "date"},
                {"name": "receiptdate", "expression": "receiptdate", "type": "date"},
                {
                    "name": "shipinstruct",
                    "expression": "shipinstruct",
                    "type": "varchar",
                },
                {"name": "shipmode", "expression": "shipmode", "type": "varchar"},
                {"name": "comment", "expression": "comment", "type": "varchar"},
            ],
        },
    ],
}

manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


def test_analysis_sql_select_all_customer():
    result = get_sql_analysis(
        {"manifestStr": manifest_str, "sql": "SELECT * FROM customer"}
    )
    assert len(result) == 1
    assert result[0]["relation"]["tableName"] == "customer"
    assert result[0]["relation"]["type"] == "TABLE"
    assert "alias" not in result[0]["relation"]
    assert len(result[0]["selectItems"]) == 8


def test_analysis_sql_group_by_customer():
    result = get_sql_analysis(
        {
            "manifestStr": manifest_str,
            "sql": "SELECT custkey, count(*) FROM customer GROUP BY 1",
        }
    )
    assert len(result) == 1
    assert result[0]["relation"]["type"] == "TABLE"
    assert "alias" not in result[0]["relation"]
    assert len(result[0]["selectItems"]) == 2
    assert result[0]["relation"]["tableName"] == "customer"
    assert len(result[0]["groupByKeys"]) == 1
    assert result[0]["groupByKeys"][0][0] == "custkey"


def test_analysis_sql_join_customer_orders():
    result = get_sql_analysis(
        {
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer c JOIN orders o ON c.custkey = o.custkey",
        }
    )
    assert len(result) == 1
    assert result[0]["relation"]["type"] == "INNER_JOIN"
    assert "alias" not in result[0]["relation"]
    assert len(result[0]["selectItems"]) == 17
    assert result[0]["relation"]["left"]["type"] == "TABLE"
    assert result[0]["relation"]["right"]["type"] == "TABLE"
    assert result[0]["relation"]["criteria"] == "ON (c.custkey = o.custkey)"
    assert result[0]["relation"]["exprSources"] == [
        {"expression": "o.custkey", "sourceDataset": "orders"},
        {"expression": "c.custkey", "sourceDataset": "customer"},
    ]


def test_analysis_sql_where_clause():
    result = get_sql_analysis(
        {
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer WHERE custkey = 1 OR (name = 'test' AND address = 'test')",
        }
    )
    assert len(result) == 1
    assert result[0]["relation"]["type"] == "TABLE"
    assert "alias" not in result[0]["relation"]
    assert len(result[0]["selectItems"]) == 8
    assert result[0]["relation"]["tableName"] == "customer"
    assert result[0]["filter"]["type"] == "OR"
    assert result[0]["filter"]["left"]["type"] == "EXPR"
    assert result[0]["filter"]["right"]["type"] == "AND"


def test_analysis_sql_group_by_multiple_columns():
    result = get_sql_analysis(
        {
            "manifestStr": manifest_str,
            "sql": "SELECT custkey, count(*), name FROM customer GROUP BY 1, 3, nationkey",
        }
    )
    assert len(result) == 1
    assert len(result[0]["groupByKeys"]) == 3
    assert result[0]["groupByKeys"][0][0] == "custkey"
    assert result[0]["groupByKeys"][1][0] == "name"
    assert result[0]["groupByKeys"][2][0] == "nationkey"


def test_analysis_sql_order_by():
    result = get_sql_analysis(
        {
            "manifestStr": manifest_str,
            "sql": "SELECT custkey, name FROM customer ORDER BY 1 ASC, 2 DESC",
        }
    )
    assert len(result) == 1
    assert len(result[0]["sortings"]) == 2
    assert result[0]["sortings"][0]["expression"] == "custkey"
    assert result[0]["sortings"][0]["ordering"] == "ASCENDING"
    assert result[0]["sortings"][1]["expression"] == "name"
    assert result[0]["sortings"][1]["ordering"] == "DESCENDING"


def get_sql_analysis(input_dto):
    response = client.request(
        method="GET",
        url="/v2/analysis/sql",
        json={
            "manifestStr": input_dto["manifestStr"],
            "sql": input_dto["sql"],
        },
    )
    assert response.status_code == 200
    return response.json()
