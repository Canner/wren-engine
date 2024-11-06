import base64

import orjson
import pytest
from fastapi.testclient import TestClient

from app.main import app
from tests.routers.v3.connector.postgres.conftest import base_url

manifest = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": {
                "schema": "public",
                "table": "orders",
            },
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
                {"name": "o_custkey", "type": "integer"},
                {
                    "name": "order_cust_key",
                    "expression": "concat(o_orderkey, '_', o_custkey)",
                    "type": "varchar",
                },
            ],
        },
    ],
}


@pytest.fixture
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


with TestClient(app) as client:

    def test_dry_plan(manifest_str):
        response = client.post(
            url=f"{base_url}/dry-plan",
            json={
                "manifestStr": manifest_str,
                "sql": "SELECT o_orderkey, order_cust_key FROM wren.public.orders LIMIT 1",
            },
        )
        assert response.status_code == 200
        assert response.text is not None
