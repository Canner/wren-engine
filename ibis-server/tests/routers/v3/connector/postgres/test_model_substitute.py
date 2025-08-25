import base64

import orjson
import pytest

from tests.routers.v3.connector.postgres.conftest import base_url

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "catalog": "test",
                "schema": "public",
                "table": "orders",
            },
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
            ],
        },
    ],
}


manifest_model_without_catalog = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
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


@pytest.fixture(scope="module")
def manifest_without_catalog_str():
    return base64.b64encode(orjson.dumps(manifest_model_without_catalog)).decode(
        "utf-8"
    )


async def test_model_substitute(
    client, manifest_str, manifest_without_catalog_str, connection_info
):
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "test"."public"."orders"',
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\""'
    )

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-catalog": "test",
            "x-user-schema": "public",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\""'
    )

    # Test only have x-user-catalog
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-catalog": "test",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 404

    # Test only have x-user-catalog but have schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-catalog": "test",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "public"."orders"',
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\""'
    )

    # Test only have x-user-schema
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-schema": "public",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 404

    # Test only have x-user-schema with no catalog mdl
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-schema": "public",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_without_catalog_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 200

    # Test empty  x-user-catalog header and x-user-schema with no catalog mdl
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-catalog": "",  # empty catalog
            "x-user-schema": "public",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_without_catalog_str,
            "sql": 'SELECT * FROM "orders"',
        },
    )
    assert response.status_code == 200


async def test_model_substitute_with_cte(client, manifest_str, connection_info):
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                WITH orders_cte AS (
                    SELECT * FROM "test"."public"."orders"
                )
                SELECT * FROM orders_cte;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"WITH orders_cte AS (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") SELECT * FROM orders_cte"'
    )

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-catalog": "test",
            "x-user-schema": "public",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                WITH orders_cte AS (
                    SELECT * FROM "orders"
                )
                SELECT * FROM orders_cte;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"WITH orders_cte AS (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") SELECT * FROM orders_cte"'
    )


async def test_model_substitute_with_subquery(client, manifest_str, connection_info):
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                SELECT * FROM (
                    SELECT * FROM "test"."public"."orders"
                ) AS orders_subquery;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") AS orders_subquery"'
    )

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-catalog": "test",
            "x-user-schema": "public",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                SELECT * FROM (
                    SELECT * FROM "orders"
                ) AS orders_subquery;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") AS orders_subquery"'
    )


async def test_model_substitute_out_of_scope(client, manifest_str, connection_info):
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Nation" LIMIT 1',
        },
    )
    assert response.status_code == 404
    assert response.json()["message"] == 'Model not found: "Nation"'

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-catalog": "test",
            "x-user-schema": "public",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Nation" LIMIT 1',
        },
    )
    assert response.status_code == 404
    assert response.json()["message"] == 'Model not found: "Nation"'


async def test_model_substitute_non_existent_column(
    client, manifest_str, connection_info
):
    # Test with catalog and schema in SQL
    response = await client.post(
        url=f"{base_url}/model-substitute",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT x FROM "test"."public"."orders" LIMIT 1',
        },
    )
    assert response.status_code == 422

    # Test without catalog and schema in SQL but in headers(x-user-xxx)
    response = await client.post(
        url=f"{base_url}/model-substitute",
        headers={
            "x-user-catalog": "test",
            "x-user-schema": "public",
        },
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT x FROM "orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
