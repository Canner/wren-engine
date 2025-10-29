import base64

import orjson
import pytest

from app.config import get_config
from tests.conftest import DATAFUSION_FUNCTION_COUNT
from tests.routers.v3.connector.snowflake.conftest import base_url, function_list_path

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
    assert len(result) == DATAFUSION_FUNCTION_COUNT + 93
    the_func = next(filter(lambda x: x["name"] == "is_null_value", result))
    assert the_func == {
        "name": "is_null_value",
        "description": "Tests if variant is SQL NULL",
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


async def test_scalar_function(client, manifest_str: str, tpch_connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": tpch_connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT ABS(-1) AS col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["COL"],
        "data": [[1]],
        "dtypes": {"COL": "int64"},
    }


async def test_aggregate_function(client, manifest_str: str, tpch_connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": tpch_connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT COUNT(*) AS col FROM (SELECT 1) AS temp_table",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["COL"],
        "data": [[1]],
        "dtypes": {"COL": "int64"},
    }
