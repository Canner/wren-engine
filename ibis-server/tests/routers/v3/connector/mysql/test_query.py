import pytest

from app.config import get_config
from app.dependencies import X_WREN_FALLBACK_DISABLE
from tests.routers.v3.connector.mysql.conftest import base_url

manifest = {
    "dataSource": "mysql",
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
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
}


@pytest.fixture(scope="module")
async def manifest_str(web_server):
    return await web_server.register_mdl(manifest)


async def test_extract(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT EXTRACT(MONTH FROM o_orderdate) AS col FROM orders LIMIT 1",
        },
        headers={X_WREN_FALLBACK_DISABLE: "true"},
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[1]],
        "dtypes": {"col": "int32"},
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT EXTRACT(WEEK FROM o_orderdate) AS col FROM orders LIMIT 1",
        },
        headers={X_WREN_FALLBACK_DISABLE: "true"},
    )
    assert response.status_code == 200
    result = response.json()
    assert result == {
        "columns": ["col"],
        "data": [[0]],
        "dtypes": {"col": "int32"},
    }
