import base64
import json

import wren_core

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "customer",
            "tableReference": {
                "schema": "main",
                "table": "customer",
            },
            "columns": [
                {"name": "c_custkey", "type": "integer"},
                {"name": "c_name", "type": "varchar"},
            ],
            "primaryKey": "c_custkey",
        },
    ],
}

manifest_str = base64.b64encode(json.dumps(manifest).encode("utf-8")).decode("utf-8")


def test_transform_sql():
    sql = "SELECT * FROM my_catalog.my_schema.customer"
    rewritten_sql = wren_core.transform_sql(manifest_str, [], sql)
    assert (
        rewritten_sql
        == 'SELECT customer.c_custkey, customer.c_name FROM (SELECT main.customer.c_custkey AS c_custkey, main.customer.c_name AS c_name FROM main.customer) AS customer'
    )

def test_read_function_list():
    path = "tests/functions.csv"
    functions = wren_core.read_remote_function_list(path)
    assert len(functions) == 3

    rewritten_sql = wren_core.transform_sql(manifest_str, functions, "SELECT add_two(c_custkey) FROM my_catalog.my_schema.customer")
    assert rewritten_sql == 'SELECT add_two(customer.c_custkey) FROM (SELECT customer.c_custkey FROM (SELECT main.customer.c_custkey AS c_custkey FROM main.customer) AS customer) AS customer'

    functions = wren_core.read_remote_function_list(None)
    assert len(functions) == 0
