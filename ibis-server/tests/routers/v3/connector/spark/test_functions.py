import base64

import orjson
import pytest

from app.config import get_config
from tests.conftest import DATAFUSION_FUNCTION_COUNT, file_path
from tests.routers.v3.connector.spark.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": {
                "schema": "default",
                "table": "orders",
            },
            "columns": [
                {"name": "orderkey", "expression": "o_orderkey", "type": "integer"},
                {"name": "custkey", "expression": "o_custkey", "type": "integer"},
                {
                    "name": "orderstatus",
                    "expression": "o_orderstatus",
                    "type": "varchar",
                },
                {"name": "totalprice", "expression": "o_totalprice", "type": "float"},
                {"name": "orderdate", "expression": "o_orderdate", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "concat(o_orderkey, '_', o_custkey)",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "cast('2024-01-01T23:59:59' as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "cast('2024-01-01T23:59:59' as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "cast(NULL as timestamp)",
                    "type": "timestamp",
                },
            ],
        },
    ],
}


function_list_path = file_path("../resources/function_list")


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    yield
    config.set_remote_function_list_path(None)


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
    assert len(result) == DATAFUSION_FUNCTION_COUNT + 491
    the_func = next(filter(lambda x: x["name"] == "bit_reverse", result))
    assert the_func == {
        "name": "bit_reverse",
        "description": "bit_reverse(expr) - Returns the value obtained by reversing the order of the bits in the two's complement binary representation of the specified integral value.",
        "function_type": "scalar",
        "param_names": None,
        "param_types": None,
        "return_type": None,
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
        "dtypes": {"col": "int32"},
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
