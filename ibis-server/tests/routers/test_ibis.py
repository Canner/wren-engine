import base64

import orjson
import pytest

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "refSql": "select * from public.orders",
            "columns": [
                {"name": "orderkey", "expression": "o_orderkey", "type": "integer"}
            ],
        },
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_dry_plan(client, manifest_str):
    response = await client.post(
        url="/v2/connector/dry-plan",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT orderkey FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 200
    assert response.text is not None
