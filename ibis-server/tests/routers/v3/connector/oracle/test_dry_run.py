import base64

import orjson
import pytest

from app.dependencies import X_WREN_FALLBACK_DISABLE
from tests.routers.v3.connector.oracle.conftest import base_url

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "dataSource": "oracle",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "schema": "SYSTEM",
                "table": "ORDERS",
            },
            "columns": [
                {"name": "orderkey", "expression": '"O_ORDERKEY"', "type": "number"},
            ],
            "primaryKey": "orderkey",
        },
        {
            "name": "NonExistentModel",
            "tableReference": {
                "schema": "SYSTEM",
                "table": "NONEXISTENT_TABLE",
            },
            "columns": [
                {"name": "id", "type": "integer"},
            ],
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_dry_run(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 204


async def test_dry_run_with_invalid_sql(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "NonExistentModel"',
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 422
    body = response.json()
    assert body["errorCode"] == "INVALID_SQL"
    assert "ORA-00942" in body["message"]
