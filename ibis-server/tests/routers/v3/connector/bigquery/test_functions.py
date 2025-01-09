import base64
import os

import orjson
import pytest

from app.config import get_config
from tests.conftest import DATAFUSION_FUNCTION_COUNT
from tests.routers.v3.connector.bigquery.conftest import base_url, function_list_path
from tests.util import FunctionCsvParser, SqlTestGenerator

pytestmark = pytest.mark.functions

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "orders",
            "tableReference": {
                "schema": "tpch_tiny",
                "table": "orders",
            },
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
            ],
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_function_list(client):
    config = get_config()

    config.set_remote_function_list_path(None)
    response = await client.get(url=f"{base_url}/functions")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == DATAFUSION_FUNCTION_COUNT

    config.set_remote_function_list_path(function_list_path)
    response = await client.get(url=f"{base_url}/functions")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == DATAFUSION_FUNCTION_COUNT + 34
    the_func = next(filter(lambda x: x["name"] == "string_agg", result))
    assert the_func == {
        "name": "string_agg",
        "description": "Aggregates string values with a delimiter.",
        "function_type": "aggregate",
        "param_names": None,
        "param_types": "text,text",
        "return_type": "text",
    }

    config.set_remote_function_list_path(None)
    response = await client.get(url=f"{base_url}/functions")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == DATAFUSION_FUNCTION_COUNT


async def test_scalar_function(client, manifest_str: str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT ABS(-1) AS col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[1]],
        "dtypes": {"col": "int64"},
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT date_add(CAST('2024-01-01' AS DATE), 1) as col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [["2024-01-02"]],
        "dtypes": {"col": "object"},
    }


async def test_aggregate_function(client, manifest_str: str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT COUNT(*) AS col FROM (SELECT 1) AS temp_table",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[1]],
        "dtypes": {"col": "int64"},
    }


async def test_functions(client, manifest_str: str, connection_info):
    csv_parser = FunctionCsvParser(os.path.join(function_list_path, "bigquery.csv"))
    sql_generator = SqlTestGenerator("bigquery")
    for function in csv_parser.parse():
        # Skip window functions util https://github.com/Canner/wren-engine/issues/924 is resolved
        if function.function_type == "window":
            continue
        # Skip functions with interval util https://github.com/Canner/wren-engine/issues/930 is resolved
        if function.name in (
            "date_add",
            "date_sub",
            "date_diff",
            "date_trunc",
            "timestamp_add",
            "timestamp_sub",
            "timestamp_diff",
            "timestamp_trunc",
        ):
            continue
        sql = sql_generator.generate_sql(function)
        response = await client.post(
            url=f"{base_url}/query",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "sql": sql,
            },
        )
        assert response.status_code == 200
