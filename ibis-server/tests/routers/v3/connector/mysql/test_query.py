import base64

import orjson
import pytest

from app.dependencies import X_WREN_FALLBACK_DISABLE
from tests.routers.v3.connector.mysql.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "json_test",
            "tableReference": {
                "table": "json_test",
            },
            "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "object_col", "type": "json"},
                {"name": "array_col", "type": "json"},
            ],
        },
        {
            "name": "orders",
            "tableReference": {
                "table": "orders",
            },
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
                {"name": "o_orderdate", "type": "date"},
            ],
        },
    ],
    "dataSource": "mysql",
}


@pytest.fixture(scope="module")
async def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_json_query(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT object_col, array_col FROM wren.public.json_test limit 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"] == [
        [
            '{"age": 30, "city": "New York", "name": "Alice"}',
            '["apple", "banana", "cherry"]',
        ]
    ]
    assert result["dtypes"] == {
        "object_col": "string",
        "array_col": "string",
    }
