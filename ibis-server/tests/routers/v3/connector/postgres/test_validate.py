import base64

import orjson
import pytest

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


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_validate_with_unknown_rule(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/validate/unknown_rule",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders", "columnName": "o_orderkey"},
        },
    )
    assert response.status_code == 404
    assert (
        response.json()["message"]
        == f"The rule `unknown_rule` is not in the rules, rules: {rules}"
    )


async def test_validate_rule_column_is_valid(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders", "columnName": "o_orderkey"},
        },
    )
    assert response.status_code == 204


async def test_validate_rule_column_is_valid_with_invalid_parameters(
    client, manifest_str, connection_info
):
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "X", "columnName": "o_orderkey"},
        },
    )
    assert response.status_code == 422

    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders", "columnName": "X"},
        },
    )
    assert response.status_code == 422


async def test_validate_rule_column_is_valid_without_parameters(
    client, manifest_str, connection_info
):
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={"connectionInfo": connection_info, "manifestStr": manifest_str},
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "parameters"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_validate_rule_column_is_valid_without_one_parameter(
    client, manifest_str, connection_info
):
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders"},
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == "columnName is required"

    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"columnName": "o_orderkey"},
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == "modelName is required"


async def test_validate_rlac_condition_syntax_is_valid(
    client, manifest_str, connection_info
):
    response = await client.post(
        url=f"{base_url}/validate/rlac_condition_syntax_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {
                "modelName": "orders",
                "requiredProperties": [
                    {"name": "session_order", "required": "false"},
                ],
                "condition": "@session_order = o_orderkey",
            },
        },
    )
    assert response.status_code == 204

    response = await client.post(
        url=f"{base_url}/validate/rlac_condition_syntax_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {
                "modelName": "orders",
                "requiredProperties": [
                    {"name": "session_order", "required": False},
                ],
                "condition": "@session_order = o_orderkey",
            },
        },
    )
    assert response.status_code == 204

    response = await client.post(
        url=f"{base_url}/validate/rlac_condition_syntax_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {
                "modelName": "orders",
                "requiredProperties": [
                    {"name": "session_order", "required": "false"},
                ],
                "condition": "@session_not_found = o_orderkey",
            },
        },
    )

    assert response.status_code == 422
    assert (
        response.json()["message"]
        == "Error during planning: The session property @session_not_found is used for `rlac_validation` rule, but not found in the session properties"
    )
