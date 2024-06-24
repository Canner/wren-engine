import base64
import json

import wren_core

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "customer",
            "tableReference": "main.customer",
            "columns": [
                {"name": "custkey", "expression": "c_custkey", "type": "integer"},
                {"name": "name", "expression": "c_name", "type": "varchar"},
            ],
            "primaryKey": "custkey",
        },
    ],
}

manifest_str = base64.b64encode(json.dumps(manifest).encode("utf-8")).decode("utf-8")


def test_transform_sql():
    sql = "SELECT * FROM my_catalog.my_schema.customer"
    rewritten_sql = wren_core.transform_sql(manifest_str, sql)
    assert (
        rewritten_sql
        == 'SELECT customer.custkey, customer."name" FROM (SELECT customer.custkey, customer."name" FROM (SELECT main.customer.c_custkey AS custkey, main.customer.c_name AS "name" FROM main.customer) AS customer) AS customer'
    )
