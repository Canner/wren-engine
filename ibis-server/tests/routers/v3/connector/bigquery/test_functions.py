import base64
import os

import orjson
import pytest
from fastapi.testclient import TestClient

from app.config import get_config
from app.main import app
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
        assert len(result) == DATAFUSION_FUNCTION_COUNT + 35
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

        response = client.post(
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

    def test_functions(manifest_str: str, connection_info):
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
            response = client.post(
                url=f"{base_url}/query",
                json={
                    "connectionInfo": connection_info,
                    "manifestStr": manifest_str,
                    "sql": sql,
                },
            )
            assert response.status_code == 200
