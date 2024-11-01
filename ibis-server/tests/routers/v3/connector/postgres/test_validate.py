import base64

import orjson
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.model.validator import rules
from tests.routers.v3.connector.postgres.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
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


@pytest.fixture
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


with TestClient(app) as client:

    def test_validate_with_unknown_rule(manifest_str, connection_info):
        response = client.post(
            url=f"{base_url}/validate/unknown_rule",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "orders", "columnName": "o_orderkey"},
            },
        )
        assert response.status_code == 404
        assert (
            response.text
            == f"The rule `unknown_rule` is not in the rules, rules: {rules}"
        )

    def test_validate_rule_column_is_valid(manifest_str, connection_info):
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "orders", "columnName": "o_orderkey"},
            },
        )
        assert response.status_code == 204

    def test_validate_rule_column_is_valid_with_invalid_parameters(
        manifest_str, connection_info
    ):
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "X", "columnName": "o_orderkey"},
            },
        )
        assert response.status_code == 422

        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "orders", "columnName": "X"},
            },
        )
        assert response.status_code == 422

    def test_validate_rule_column_is_valid_without_parameters(
        manifest_str, connection_info
    ):
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={"connectionInfo": connection_info, "manifestStr": manifest_str},
        )
        assert response.status_code == 422
        result = response.json()
        assert result["detail"][0] is not None
        assert result["detail"][0]["type"] == "missing"
        assert result["detail"][0]["loc"] == ["body", "parameters"]
        assert result["detail"][0]["msg"] == "Field required"

    def test_validate_rule_column_is_valid_without_one_parameter(
        manifest_str, connection_info
    ):
        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"modelName": "orders"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `columnName`"

        response = client.post(
            url=f"{base_url}/validate/column_is_valid",
            json={
                "connectionInfo": connection_info,
                "manifestStr": manifest_str,
                "parameters": {"columnName": "o_orderkey"},
            },
        )
        assert response.status_code == 422
        assert response.text == "Missing required parameter: `modelName`"
