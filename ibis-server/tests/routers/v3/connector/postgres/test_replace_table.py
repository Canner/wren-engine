import base64

import orjson
import pytest
from routers.v3.connector.postgres.conftest import base_url

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "schema": "tpch_tiny",
                "table": "orders",
            },
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_replace_table(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/replace-table",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "tpch_tiny"."orders"',
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\""'
    )


async def test_replace_table_with_cte(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/replace-table",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                WITH orders_cte AS (
                    SELECT * FROM "tpch_tiny"."orders"
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


async def test_replace_table_with_subquery(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/replace-table",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
                SELECT * FROM (
                    SELECT * FROM "tpch_tiny"."orders"
                ) AS orders_subquery;
            """,
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '"SELECT * FROM (SELECT * FROM \\"my_catalog\\".\\"my_schema\\".\\"Orders\\" AS \\"orders\\") AS orders_subquery"'
    )


async def test_replace_table_out_of_scope(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/replace-table",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Nation" LIMIT 1',
        },
    )
    assert response.status_code == 422
    assert response.text == 'Model not found: "Nation"'
