import base64
import json

import wren_core
from wren_core import SessionContext

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

def test_session_context():
    session_context = SessionContext(manifest_str, None)
    sql = "SELECT * FROM my_catalog.my_schema.customer"
    rewritten_sql = session_context.transform_sql(sql)
    assert (
            rewritten_sql
            == 'SELECT customer.c_custkey, customer.c_name FROM (SELECT customer.c_custkey AS c_custkey, customer.c_name AS c_name FROM main.customer) AS customer'
    )

    session_context = SessionContext(manifest_str, "tests/functions.csv")
    sql = "SELECT add_two(c_custkey) FROM my_catalog.my_schema.customer"
    rewritten_sql = session_context.transform_sql(sql)
    assert (
            rewritten_sql
            == 'SELECT add_two(customer.c_custkey) FROM (SELECT customer.c_custkey FROM (SELECT customer.c_custkey AS c_custkey FROM main.customer) AS customer) AS customer'
    )

def test_read_function_list():
    path = "tests/functions.csv"
    session_context = SessionContext(manifest_str, path)
    functions = session_context.get_available_functions()
    assert len(functions) == 271

    rewritten_sql = session_context.transform_sql("SELECT add_two(c_custkey) FROM my_catalog.my_schema.customer")
    assert rewritten_sql == 'SELECT add_two(customer.c_custkey) FROM (SELECT customer.c_custkey FROM (SELECT customer.c_custkey AS c_custkey FROM main.customer) AS customer) AS customer'

    session_context = SessionContext(manifest_str, None)
    functions = session_context.get_available_functions()
    assert len(functions) == 269


def test_get_available_functions():
    session_context = SessionContext(manifest_str, "tests/functions.csv")
    functions = session_context.get_available_functions()
    add_two = next(filter(lambda x: x["name"] == "add_two", map(lambda x: x.to_dict(), functions)))
    assert add_two["name"] == "add_two"
    assert add_two["function_type"] == "scalar"
    assert add_two["description"] == "Adds two numbers together."
    assert add_two["return_type"] == "int"
    assert add_two["param_names"] == "f1,f2"
    assert add_two["param_types"] == "int,int"

    max_if = next(filter(lambda x: x["name"] == "max_if", map(lambda x: x.to_dict(), functions)))
    assert max_if["name"] == "max_if"
    assert max_if["function_type"] == "window"
    assert max_if["param_names"] is None
    assert max_if["param_types"] is None

