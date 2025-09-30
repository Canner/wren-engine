import base64

import orjson
import pytest

from app.config import get_config
from app.dependencies import X_WREN_FALLBACK_DISABLE
from tests.conftest import DATAFUSION_FUNCTION_COUNT
from tests.routers.v3.connector.bigquery.conftest import (
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
                "schema": "tpch_tiny",
                "table": "orders",
            },
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
                {"name": "o_orderdate", "type": "date"},
            ],
        },
    ],
    "dataSource": "bigquery",
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
    assert len(result) == 179
    the_func = next(
        filter(
            lambda x: x["name"] == "string_agg",
            result,
        )
    )
    assert the_func == {
        "name": "string_agg",
        "description": "Concatenates strings with a separator",
        "function_type": "aggregate",
        "param_names": None,
        "param_types": None,
        "return_type": "string",
    }

    config.set_remote_function_list_path(None)
    config.set_remote_white_function_list_path(None)
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
            "sql": "SELECT date_add(CAST('2024-01-01' AS DATE), INTERVAL 1 DAY) as col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [["2024-01-02"]],
        "dtypes": {"col": "date32[day]"},
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


async def test_datetime_function(client, manifest_str: str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT CURRENT_TIMESTAMP() AS col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[result["data"][0][0]]],
        "dtypes": {"col": "timestamp[us, tz=UTC]"},
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT DATETIME('2001-01-01 00:11:11') AS col",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [["2001-01-01 00:11:11.000000"]],
        "dtypes": {"col": "timestamp[us]"},
    }


async def test_date_diff_function(client, manifest_str: str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT DATE_DIFF(DAY, CURRENT_DATE(), o_orderdate) AS col FROM orders LIMIT 1",
        },
    )

    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[result["data"][0][0]]],
        "dtypes": {"col": "int64"},
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT DATEDIFF(DAY, CURRENT_DATE(), o_orderdate) AS col FROM orders LIMIT 1",
        },
    )

    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[result["data"][0][0]]],
        "dtypes": {"col": "int64"},
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT DATEDIFF('DAY', CURRENT_DATE(), o_orderdate) AS col FROM orders LIMIT 1",
        },
    )

    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[result["data"][0][0]]],
        "dtypes": {"col": "int64"},
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT DATEDIFF('DAYS', CURRENT_DATE(), o_orderdate) AS col FROM orders LIMIT 1",
        },
    )

    assert response.status_code == 422
    assert "Unsupported date part 'DAYS' for BIGQUERY." in response.text

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT DATEDIFF('HOUR', CURRENT_TIMESTAMP(), TIMESTAMP WITH TIME ZONE '2025-01-01 00:00:00') AS col FROM orders LIMIT 1",
        },
    )

    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[result["data"][0][0]]],
        "dtypes": {"col": "int64"},
    }


async def test_query_safe_divide(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT safe_divide(10.0, 2.0) as result",
        },
        headers={X_WREN_FALLBACK_DISABLE: "true"},
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert result["data"][0][0] == 5.0


# Using pytest.mark.parametrize to refactor the temporal function test
@pytest.mark.parametrize(
    "sql,expected_column",
    [
        (
            "SELECT date_add(CURRENT_DATE(), INTERVAL 30 DAY) as future_date",
            "future_date",
        ),
        ("SELECT date_sub(CURRENT_DATE(), INTERVAL 30 DAY) as past_date", "past_date"),
        (
            "SELECT datetime_add(CAST('2024-01-01 10:00:00' AS TIMESTAMP), INTERVAL 5 DAY) as future_datetime",
            "future_datetime",
        ),
        (
            "SELECT datetime_sub(CAST('2024-01-01 10:00:00' AS TIMESTAMP), INTERVAL 5 DAY) as past_datetime",
            "past_datetime",
        ),
        (
            "SELECT timestamp_add(CAST('2024-01-01 10:00:00+00' AS TIMESTAMP WITH TIME ZONE), INTERVAL 5 DAY) as future_timestamp",
            "future_timestamp",
        ),
        (
            "SELECT timestamp_sub(CAST('2024-01-01 10:00:00+00' AS TIMESTAMP WITH TIME ZONE), INTERVAL 5 DAY) as past_timestamp",
            "past_timestamp",
        ),
    ],
)
async def test_temporal_functions(
    client, manifest_str, connection_info, sql, expected_column
):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": sql,
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert expected_column in result["columns"]
