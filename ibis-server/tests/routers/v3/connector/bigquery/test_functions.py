import base64

import orjson
import pytest
from fastapi.testclient import TestClient

from app.config import get_config
from app.main import app
from tests.routers.v3.connector.bigquery.conftest import base_url, function_list_path
from tests.conftest import DATAFUSION_FUNCTION_COUNT

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


@pytest.fixture
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


with TestClient(app) as client:

    def test_function_list():
        config = get_config()

        config.set_remote_function_list_path(None)
        response = client.get(url=f"{base_url}/functions")
        assert response.status_code == 200
        result = response.json()
        assert len(result) == DATAFUSION_FUNCTION_COUNT

        config.set_remote_function_list_path(function_list_path)
        response = client.get(url=f"{base_url}/functions")
        assert response.status_code == 200
        result = response.json()
        assert len(result) == DATAFUSION_FUNCTION_COUNT + 33
        the_func = next(filter(lambda x: x["name"] == "abs", result))
        assert the_func == {
            "name": "abs",
            "description": "Returns the absolute value of a number.",
            "function_type": "scalar",
            "param_names": None,
            "param_types": None,
            "return_type": "FLOAT64",
        }

        config.set_remote_function_list_path(None)
        response = client.get(url=f"{base_url}/functions")
        assert response.status_code == 200
        result = response.json()
        assert len(result) == DATAFUSION_FUNCTION_COUNT

    def test_scalar_function(manifest_str: str, connection_info):
        response = client.post(
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

    def test_aggregate_function(manifest_str: str, connection_info):
        response = client.post(
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
