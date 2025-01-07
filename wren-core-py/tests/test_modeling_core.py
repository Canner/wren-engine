import base64
import json
from contextlib import nullcontext as does_not_raise

import pytest
from wren_core import (
    ManifestExtractor,
    SessionContext,
    to_json_base64,
)

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "dataSource": "bigquery",
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
                {"name": "orders", "type": "orders", "relationship": "orders_customer"},
            ],
            "primaryKey": "c_custkey",
        },
        {
            "name": "orders",
            "tableReference": {
                "schema": "main",
                "table": "orders",
            },
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
                {"name": "o_custkey", "type": "integer"},
                {"name": "o_orderdate", "type": "date"},
                {
                    "name": "lineitems",
                    "type": "Lineitem",
                    "relationship": "orders_lineitem",
                },
            ],
            "primaryKey": "o_orderkey",
        },
        {
            "name": "lineitem",
            "tableReference": {
                "schema": "main",
                "table": "lineitem",
            },
            "columns": [
                {"name": "l_orderkey", "type": "integer"},
                {"name": "l_quantity", "type": "decimal"},
                {"name": "l_extendedprice", "type": "decimal"},
            ],
            "primaryKey": "l_orderkey",
        },
    ],
    "relationships": [
        {
            "name": "orders_customer",
            "models": ["orders", "customer"],
            "joinType": "MANY_TO_ONE",
            "condition": "orders.custkey = customer.custkey",
        },
        {
            "name": "orders_lineitem",
            "models": ["orders", "lineitem"],
            "joinType": "ONE_TO_MANY",
            "condition": "orders.orderkey = lineitem.orderkey",
        },
    ],
    "views": [
        {
            "name": "customer_view",
            "statement": "SELECT * FROM my_catalog.my_schema.customer",
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
        == "SELECT customer.c_custkey, customer.c_name FROM (SELECT __source.c_custkey AS c_custkey, __source.c_name AS c_name FROM main.customer AS __source) AS customer"
    )

    session_context = SessionContext(manifest_str, "tests/functions.csv")
    sql = "SELECT add_two(c_custkey) FROM my_catalog.my_schema.customer"
    rewritten_sql = session_context.transform_sql(sql)
    assert (
        rewritten_sql
        == "SELECT add_two(customer.c_custkey) FROM (SELECT customer.c_custkey FROM (SELECT __source.c_custkey AS c_custkey FROM main.customer AS __source) AS customer) AS customer"
    )


def test_read_function_list():
    path = "tests/functions.csv"
    session_context = SessionContext(manifest_str, path)
    functions = session_context.get_available_functions()
    assert len(functions) == 273

    rewritten_sql = session_context.transform_sql(
        "SELECT add_two(c_custkey) FROM my_catalog.my_schema.customer"
    )
    assert (
        rewritten_sql
        == "SELECT add_two(customer.c_custkey) FROM (SELECT customer.c_custkey FROM (SELECT __source.c_custkey AS c_custkey FROM main.customer AS __source) AS customer) AS customer"
    )

    session_context = SessionContext(manifest_str, None)
    functions = session_context.get_available_functions()
    assert len(functions) == 271


def test_get_available_functions():
    session_context = SessionContext(manifest_str, "tests/functions.csv")
    functions = session_context.get_available_functions()
    add_two = next(f for f in functions if f.name == "add_two")
    assert add_two.name == "add_two"
    assert add_two.function_type == "scalar"
    assert add_two.description == "Adds two numbers together."
    assert add_two.return_type == "int"
    assert add_two.param_names == "f1,f2"
    assert add_two.param_types == "int,int"

    max_if = next(f for f in functions if f.name == "max_if")
    assert max_if.name == "max_if"
    assert max_if.function_type == "window"
    assert max_if.param_names is None
    assert max_if.param_types is None


@pytest.mark.parametrize(
    ("value", "expected_error", "error_message"),
    [
        (
            None,
            Exception,
            "Expected a valid base64 encoded string for the model definition, but got None.",
        ),
        ("xxx", Exception, "Base64 decode error: Invalid padding"),
        ("{}", Exception, "Base64 decode error: Invalid symbol 123, offset 0."),
        (
            "",
            Exception,
            "Serde JSON error: EOF while parsing a value at line 1 column 0",
        ),
    ],
)
def test_extractor_with_invalid_manifest(value, expected_error, error_message):
    with pytest.raises(expected_error) as e:
        ManifestExtractor(value)
    assert str(e.value) == error_message


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM customer", ["customer"]),
        ("SELECT * FROM not_my_catalog.my_schema.customer", []),
        ("SELECT * FROM my_catalog.not_my_schema.customer", []),
        ("SELECT * FROM my_catalog.my_schema.customer", ["customer"]),
        (
            "SELECT * FROM my_catalog.my_schema.customer JOIN my_catalog.my_schema.orders ON customer.custkey = orders.custkey",
            ["customer", "orders"],
        ),
        ("SELECT * FROM my_catalog.my_schema.customer_view", ["customer_view"]),
    ],
)
def test_resolve_used_table_names(sql, expected):
    tables = ManifestExtractor(manifest_str).resolve_used_table_names(sql)
    assert tables == expected


@pytest.mark.parametrize(
    ("dataset", "expected_models"),
    [
        (["customer"], ["customer", "orders", "lineitem"]),
        (["customer_view"], ["customer", "orders", "lineitem"]),
        (["orders"], ["orders", "lineitem"]),
        (["lineitem"], ["lineitem"]),
    ],
)
def test_extract_by(dataset, expected_models):
    extracted_manifest = ManifestExtractor(manifest_str).extract_by(dataset)
    assert len(extracted_manifest.models) == len(expected_models)
    assert [m.name for m in extracted_manifest.models] == expected_models
    assert extracted_manifest.data_source.__str__() == "DataSource.BigQuery"


def test_to_json_base64():
    extracted_manifest = ManifestExtractor(manifest_str).extract_by(["customer"])
    base64_str = to_json_base64(extracted_manifest)
    with does_not_raise():
        json_str = base64.b64decode(base64_str)
        decoded_manifest = json.loads(json_str)
        assert decoded_manifest["catalog"] == "my_catalog"
        assert len(decoded_manifest["models"]) == 3
