import base64

import orjson
import pytest

from app.dependencies import (
    X_WREN_FALLBACK_DISABLE,
)
from tests.routers.v3.connector.mssql.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "unicode_test",
            "tableReference": {
                "schema": "dbo",
                "table": "unicode_test",
            },
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "letter", "type": "varchar"},
            ],
        }
    ],
    "dataSource": "MSSQL",
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_unicode_literal(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT id FROM wren.public.unicode_test WHERE letter = '真夜中'",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"] == [[1]]

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT id FROM wren.public.unicode_test WHERE letter = 'ZUTOMAYO'",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"] == [[2]]


async def test_pagination(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM (SELECT id FROM wren.public.unicode_test WHERE id < 3 ORDER BY id) LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM (SELECT id FROM wren.public.unicode_test WHERE id < 3 ORDER BY id) as _paginated_table LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"] == [[1]]

    # multiple subqueries - should fail
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM (SELECT id FROM wren.public.unicode_test WHERE id < 3 ORDER BY id),  (SELECT letter FROM wren.public.unicode_test WHERE id < 3 ORDER BY id) as _paginated_table LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 422
