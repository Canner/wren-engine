import base64

from orjson import orjson

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

manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


def test_transform_sql():
    sql = "SELECT * FROM my_catalog.my_schema.customer"
    rewritten_sql = wren_core.transform_sql(manifest_str, sql)
    assert (
        rewritten_sql
        == 'SELECT "my_catalog"."my_schema"."customer"."custkey", "my_catalog"."my_schema"."customer"."name" FROM (SELECT "customer"."c_custkey" AS "custkey", "customer"."c_name" AS "name" FROM "customer") AS "customer"'
    )