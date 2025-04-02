import base64

import orjson
import pytest

from app.config import get_config
from tests.conftest import DATAFUSION_FUNCTION_COUNT, file_path
from tests.routers.v3.connector.local_file.conftest import base_url

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "table": "tests/resource/tpch/data/orders.parquet",
            },
            "columns": [
                {"name": "orderkey", "expression": "o_orderkey", "type": "integer"},
            ],
            "primaryKey": "orderkey",
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
    # 429 is the number of functions in `resources/function_list/duckdb.csv` file excluded the default functions in DataFusion.
    assert len(result) == DATAFUSION_FUNCTION_COUNT + 429
    the_func = next(filter(lambda x: x["name"] == "regexp_escape", result))
    assert the_func == {
        "name": "regexp_escape",
        "description": "Escapes all potentially meaningful regexp characters in the input string",
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
