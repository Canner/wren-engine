import base64

import orjson
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

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
            "primaryKey": "orderkey",
        },
    ],
}

manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


def test_dry_plan():
    response = client.post(
        url="/v2/connector/dry-plan",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT orderkey FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 200
    assert (
        response.text
        == '''"WITH\\n  \\"Orders\\" AS (\\n   SELECT \\"Orders\\".\\"orderkey\\" \\"orderkey\\"\\n   FROM\\n     (\\n      SELECT \\"Orders\\".\\"orderkey\\" \\"orderkey\\"\\n      FROM\\n        (\\n         SELECT o_orderkey \\"orderkey\\"\\n         FROM\\n           (\\n            SELECT *\\n            FROM\\n              public.orders\\n         )  \\"Orders\\"\\n      )  \\"Orders\\"\\n   )  \\"Orders\\"\\n) \\nSELECT orderkey\\nFROM\\n  \\"Orders\\"\\nLIMIT 1\\n"'''
    )
