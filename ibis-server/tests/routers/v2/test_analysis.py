import base64

import pytest
from orjson import orjson

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


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_analysis_sql_select_all_customer(client, manifest_str):
    result = await get_sql_analysis(
        client, {"manifestStr": manifest_str, "sql": "SELECT * FROM customer"}
    )
    assert len(result) == 1
    assert result[0]["relation"]["tableName"] == "customer"
    assert result[0]["relation"]["type"] == "TABLE"
    assert "alias" not in result[0]["relation"]
    assert len(result[0]["selectItems"]) == 8
    assert result[0]["selectItems"][0]["nodeLocation"] == {"line": 1, "column": 8}
    assert result[0]["selectItems"][0]["exprSources"] == [
        {
            "expression": "custkey",
            "nodeLocation": {"line": 1, "column": 8},
            "sourceDataset": "customer",
            "sourceColumn": "custkey",
        },
    ]
    assert result[0]["selectItems"][1]["nodeLocation"] == {"line": 1, "column": 8}
    assert result[0]["relation"]["tableName"] == "customer"
    assert result[0]["relation"]["nodeLocation"] == {"line": 1, "column": 15}


async def test_analysis_sql_group_by_customer(client, manifest_str):
    result = await get_sql_analysis(
        client,
        {
            "manifestStr": manifest_str,
            "sql": "SELECT custkey, count(*) FROM customer GROUP BY 1",
        },
    )
    assert len(result) == 1
    assert result[0]["relation"]["type"] == "TABLE"
    assert "alias" not in result[0]["relation"]
    assert len(result[0]["selectItems"]) == 2
    assert result[0]["selectItems"][0]["nodeLocation"] == {"line": 1, "column": 8}
    assert result[0]["selectItems"][1]["nodeLocation"] == {"line": 1, "column": 17}
    assert result[0]["relation"]["tableName"] == "customer"
    assert result[0]["relation"]["nodeLocation"] == {"line": 1, "column": 31}
    assert len(result[0]["groupByKeys"]) == 1
    assert result[0]["groupByKeys"][0][0] == {
        "expression": "custkey",
        "nodeLocation": {"line": 1, "column": 49},
        "exprSources": [
            {
                "expression": "custkey",
                "nodeLocation": {"line": 1, "column": 8},
                "sourceDataset": "customer",
                "sourceColumn": "custkey",
            },
        ],
    }


async def test_analysis_sql_join_customer_orders(client, manifest_str):
    result = await get_sql_analysis(
        client,
        {
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer c JOIN orders o ON c.custkey = o.custkey",
        },
    )
    assert len(result) == 1
    assert result[0]["relation"]["type"] == "INNER_JOIN"
    assert "alias" not in result[0]["relation"]
    assert result[0]["relation"].get("tableName") is None
    assert result[0]["relation"]["nodeLocation"] == {"line": 1, "column": 15}
    assert len(result[0]["selectItems"]) == 17
    assert result[0]["relation"]["left"]["type"] == "TABLE"
    assert result[0]["relation"]["left"]["nodeLocation"] == {
        "line": 1,
        "column": 15,
    }
    assert result[0]["relation"]["right"]["type"] == "TABLE"
    assert result[0]["relation"]["right"]["nodeLocation"] == {
        "line": 1,
        "column": 31,
    }
    assert (
        result[0]["relation"]["criteria"]["expression"] == "ON (c.custkey = o.custkey)"
    )
    assert result[0]["relation"]["criteria"]["nodeLocation"] == {
        "line": 1,
        "column": 43,
    }
    assert result[0]["relation"]["exprSources"] == [
        {
            "expression": "c.custkey",
            "nodeLocation": {"line": 1, "column": 43},
            "sourceDataset": "customer",
            "sourceColumn": "custkey",
        },
        {
            "expression": "o.custkey",
            "nodeLocation": {"line": 1, "column": 55},
            "sourceDataset": "orders",
            "sourceColumn": "custkey",
        },
    ]


async def test_analysis_sql_where_clause(client, manifest_str):
    result = await get_sql_analysis(
        client,
        {
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer WHERE custkey = 1 OR (name = 'test' AND address = 'test')",
        },
    )
    assert len(result) == 1
    assert result[0]["relation"]["type"] == "TABLE"
    assert "alias" not in result[0]["relation"]
    assert len(result[0]["selectItems"]) == 8
    assert result[0]["relation"]["tableName"] == "customer"
    assert result[0]["filter"]["type"] == "OR"
    assert result[0]["filter"]["left"]["type"] == "EXPR"
    assert result[0]["filter"]["left"]["exprSources"] == [
        {
            "expression": "custkey",
            "nodeLocation": {"line": 1, "column": 30},
            "sourceDataset": "customer",
            "sourceColumn": "custkey",
        },
    ]
    assert result[0]["filter"]["right"]["type"] == "AND"


async def test_analysis_sql_group_by_multiple_columns(client, manifest_str):
    result = await get_sql_analysis(
        client,
        {
            "manifestStr": manifest_str,
            "sql": "SELECT custkey, count(*), name FROM customer GROUP BY 1, 3, nationkey",
        },
    )
    assert len(result) == 1
    assert len(result[0]["groupByKeys"]) == 3
    assert result[0]["groupByKeys"][0][0] == {
        "expression": "custkey",
        "nodeLocation": {"line": 1, "column": 55},
        "exprSources": [
            {
                "expression": "custkey",
                "nodeLocation": {"line": 1, "column": 8},
                "sourceDataset": "customer",
                "sourceColumn": "custkey",
            },
        ],
    }
    assert result[0]["groupByKeys"][1][0] == {
        "expression": "name",
        "nodeLocation": {"line": 1, "column": 58},
        "exprSources": [
            {
                "expression": "name",
                "nodeLocation": {"line": 1, "column": 27},
                "sourceDataset": "customer",
                "sourceColumn": "name",
            },
        ],
    }
    assert result[0]["groupByKeys"][2][0] == {
        "expression": "nationkey",
        "nodeLocation": {"line": 1, "column": 61},
        "exprSources": [
            {
                "expression": "nationkey",
                "nodeLocation": {"line": 1, "column": 61},
                "sourceDataset": "customer",
                "sourceColumn": "nationkey",
            },
        ],
    }


async def test_analysis_sql_order_by(client, manifest_str):
    result = await get_sql_analysis(
        client,
        {
            "manifestStr": manifest_str,
            "sql": "SELECT custkey, name FROM customer ORDER BY 1 ASC, 2 DESC",
        },
    )
    assert len(result) == 1
    assert len(result[0]["sortings"]) == 2
    assert result[0]["sortings"][0]["expression"] == "custkey"
    assert result[0]["sortings"][0]["ordering"] == "ASCENDING"
    assert result[0]["sortings"][0]["exprSources"] == [
        {
            "expression": "custkey",
            "nodeLocation": {"line": 1, "column": 8},
            "sourceDataset": "customer",
            "sourceColumn": "custkey",
        },
    ]
    assert result[0]["sortings"][0]["nodeLocation"] == {"line": 1, "column": 45}
    assert result[0]["sortings"][1]["expression"] == "name"
    assert result[0]["sortings"][1]["ordering"] == "DESCENDING"
    assert result[0]["sortings"][1]["nodeLocation"] == {"line": 1, "column": 52}


async def test_analysis_sqls(client, manifest_str):
    result = await get_sql_analysis_batch(
        client,
        {
            "manifestStr": manifest_str,
            "sqls": [
                "SELECT * FROM customer",
                "SELECT custkey, count(*) FROM customer GROUP BY 1",
                "WITH t1 AS (SELECT * FROM customer) SELECT * FROM t1",
                "SELECT * FROM orders WHERE orderkey = 1 UNION SELECT * FROM orders where orderkey = 2",
            ],
        },
    )
    assert len(result) == 4
    assert len(result[0]) == 1
    assert result[0][0]["relation"]["tableName"] == "customer"
    assert len(result[1]) == 1
    assert result[1][0]["relation"]["tableName"] == "customer"
    assert len(result[2]) == 2
    assert result[2][0]["relation"]["tableName"] == "customer"
    assert result[2][1]["relation"]["tableName"] == "t1"
    assert len(result[3]) == 2
    assert result[3][0]["relation"]["tableName"] == "orders"
    assert result[3][1]["relation"]["tableName"] == "orders"


async def get_sql_analysis(client, input_dto):
    response = await client.request(
        method="GET",
        url="/v2/analysis/sql",
        json={
            "manifestStr": input_dto["manifestStr"],
            "sql": input_dto["sql"],
        },
    )
    assert response.status_code == 200
    return response.json()


async def get_sql_analysis_batch(client, input_dto):
    response = await client.request(
        method="GET",
        url="/v2/analysis/sqls",
        json={
            "manifestStr": input_dto["manifestStr"],
            "sqls": input_dto["sqls"],
        },
    )
    assert response.status_code == 200
    return response.json()
