import base64

import orjson
import pytest

from app.dependencies import X_WREN_FALLBACK_DISABLE, X_WREN_VARIABLE_PREFIX
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

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 422


async def test_query_with_cache(client, manifest_str, connection_info):
    # First request - should miss cache and fallback to v2
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()

    # Second request with same SQL - should hit cache from v2 fallback
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )

    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"
    assert int(response2.headers["X-Cache-Create-At"]) > 1743984000  # 2025.04.07
    result2 = response2.json()

    # Verify results are identical
    assert result1["data"] == result2["data"]
    assert result1["columns"] == result2["columns"]
    assert result1["dtypes"] == result2["dtypes"]

    # we disable the fallback, we should miss the cache
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response3.status_code == 422


async def test_query_with_cache_override(client, manifest_str, connection_info):
    # First request - should miss cache then create cache via v2 fallback
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response1.status_code == 200

    # Second request with same SQL - should hit cache and override it via v2 fallback
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true&overrideCache=true",  # Override the cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Override"] == "true"
    assert int(response2.headers["X-Cache-Override-At"]) > int(
        response2.headers["X-Cache-Create-At"]
    )

    # Test that the cached result can be retrieved even with fallback disabled
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    # Currently fails with 422 because we include fallback header to cache key
    assert response3.status_code == 422


async def test_query_with_connection_url(client, manifest_str, connection_url):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response.status_code == 200

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 422


async def test_query_with_connection_url_and_cache_enable(
    client, manifest_str, connection_url
):
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()

    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )

    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"
    result2 = response2.json()

    # Verify results are identical
    assert result1["data"] == result2["data"]
    assert result1["columns"] == result2["columns"]
    assert result1["dtypes"] == result2["dtypes"]

    # Test that the cached result can be retrieved even with fallback disabled
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    # Currently fails with 422 because we include fallback header to cache key
    assert response3.status_code == 422


async def test_query_with_connection_url_and_cache_override(
    client, manifest_str, connection_url
):
    # First request - should miss cache then create cache via v2 fallback
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response1.status_code == 200

    # Second request with same SQL - should hit cache and override it via v2 fallback
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true&overrideCache=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Override"] == "true"
    assert int(response2.headers["X-Cache-Override-At"]) > int(
        response2.headers["X-Cache-Create-At"]
    )

    # Test that the cached result can be retrieved even with fallback disabled
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    # Currently fails with 422 because we include fallback header to cache key
    assert response3.status_code == 422


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

    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 422


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

    response = await client.post(
        url="/v3/connector/dry-plan",
        json={
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 422


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

    response = await client.post(
        url=f"{base_url}/dry-plan",
        json={
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 422


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

    response = await client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "orders", "columnName": "orderkey"},
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 422


async def test_query_rlac(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
        headers={X_WREN_VARIABLE_PREFIX + "session_user": "1"},
    )
    assert response.status_code == 200

    manifest_rlac = {
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
                "rowLevelAccessControls": [
                    {
                        "name": "rule",
                        "requiredProperties": [
                            {
                                "name": "session_user",
                                "required": False,
                            }
                        ],
                        "condition": "orderkey = @session_user",
                    },
                ],
            }
        ],
    }

    manifest_rlac_str = base64.b64encode(orjson.dumps(manifest_rlac)).decode("utf-8")

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_rlac_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response.status_code == 422

    manifest_clac = {
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
                    },
                    {
                        "name": "custkey",
                        "type": "varchar",
                        "columnLevelAccessControl": {
                            "name": "o_custkey_access",
                            "requiredProperties": [
                                {
                                    "name": "session_level",
                                    "required": False,
                                    "defaultExpr": "2",
                                }
                            ],
                            "operator": "GREATER_THAN",
                            "threshold": "3",
                        },
                        "expression": "cast(o_custkey as varchar)",
                    },
                ],
            }
        ],
    }

    manifest_clac_str = base64.b64encode(orjson.dumps(manifest_clac)).decode("utf-8")
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_clac_str,
            "sql": "SELECT orderkey FROM orders LIMIT 1",
        },
    )
    assert response.status_code == 422
