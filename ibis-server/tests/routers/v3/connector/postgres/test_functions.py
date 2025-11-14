import base64

import orjson
import pytest

from app.config import get_config
from tests.conftest import DATAFUSION_FUNCTION_COUNT
from tests.routers.v3.connector.postgres.conftest import (
    base_url,
    function_list_path,
    white_function_list_path,
)

pytestmark = pytest.mark.functions

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "orders",
            "tableReference": {
                "schema": "public",
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
    config.set_remote_white_function_list_path(None)
    response = await client.get(url=f"{base_url}/functions")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == DATAFUSION_FUNCTION_COUNT

    config.set_remote_function_list_path(function_list_path)
    config.set_remote_white_function_list_path(white_function_list_path)
    response = await client.get(url=f"{base_url}/functions")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == 227
    the_func = next(filter(lambda x: x["name"] == "extract", result))
    assert the_func == {
        "name": "extract",
        "description": "Get subfield from date/time",
        "function_type": "scalar",
        "param_names": None,
        "param_types": "text,timestamp",
        "return_type": "numeric",
    }

    config.set_remote_function_list_path(None)
    config.set_remote_white_function_list_path(None)
    response = await client.get(url=f"{base_url}/functions")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == DATAFUSION_FUNCTION_COUNT


async def test_get_function(client):
    response = await client.get(url=f"{base_url}/function/div")
    assert response.status_code == 200
    result = response.json()
    assert result == [
        {
            "name": "div",
            "description": "trunc(x/y)",
            "function_type": "scalar",
            "param_names": None,
            "param_types": "Decimal(38, 10),Decimal(38, 10)",
            "return_type": "Decimal(38, 10)",
        }
    ]


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
