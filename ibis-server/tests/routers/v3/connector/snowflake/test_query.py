import base64

import orjson
import pytest

from app.dependencies import X_WREN_FALLBACK_DISABLE
from tests.routers.v3.connector.snowflake.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "car_sales",
            "tableReference": {
                "catalog": "wren",
                "schema": "PUBLIC",
                "table": "car_sales",
            },
            "columns": [
                {"name": "src", "type": "variant"},
            ],
        },
    ],
    "dataSource": "snowflake",
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_qeury(client, manifest_str, snowflake_connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": snowflake_connection_info,
            "manifestStr": manifest_str,
            "sql": "select t.a from car_sales c, UNNEST(to_array(get_path(c.src, 'customer'))) t(a)",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 2
    assert result["data"] == [
        [
            '{"address":"San Francisco, CA","name":"Joyce Ridgely","phone":"16504378889"}'
        ],
        [
            '{"address":"New York, NY","name":"Bradley Greenbloom","phone":"12127593751"}'
        ],
    ]
