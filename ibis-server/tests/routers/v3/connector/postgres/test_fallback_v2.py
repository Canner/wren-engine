import base64

import orjson
import pytest

from tests.routers.v3.connector.postgres.conftest import base_url

# It's not a valid manifest for v3. We expect the query to fail and fallback to v2.
manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": {"schema": "public", "table": "orders"},
            "columns": [
                {
                    "name": "orderkey",
                    "type": "varchar",
                    "expression": "cast(o_orderkey as varchar)",
                }
            ],
        }
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_query(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1


async def test_dry_run(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response.status_code == 204


async def test_dry_plan(client, manifest_str):
    response = await client.post(
        url="/v3/connector/dry-plan",
        json={
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    assert response.text is not None


async def test_dry_plan_for_data_source(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/dry-plan",
        json={
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    assert response.text is not None


async def test_validate(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 204
