import base64

from orjson import orjson

from wren_engine import modeling_core

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
    x = modeling_core.builder_from_base64(manifest_str)
    print(x)
    rewritten_sql = modeling_core.transform_sql(manifest_str, sql)
    assert (
        rewritten_sql
        == 'SELECT "my_catalog"."my_schema"."customer"."custkey", "my_catalog"."my_schema"."customer"."name" FROM (SELECT "customer"."c_custkey" AS "custkey", "customer"."c_name" AS "name" FROM "customer") AS "customer"'
    )
