import base64
import os

import pytest
from fastapi.testclient import TestClient
from orjson import orjson

from app.main import app

pytestmark = pytest.mark.canner

client = TestClient(app)

base_url = "/v2/connector/canner"

connection_info = {
    "host": os.getenv("CANNER_HOST", default="localhost"),
    "port": os.getenv("CANNER_PORT", default="7432"),
    "user": os.getenv("CANNER_USER", default="canner"),
    "pat": os.getenv("CANNER_PAT", default="PAT"),
    "workspace": os.getenv("CANNER_WORKSPACE", default="ws"),
}

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "refSql": f"select * from canner.{connection_info['workspace']}.orders",
            "columns": [
                {"name": "orderkey", "expression": "o_orderkey", "type": "integer"},
                {"name": "custkey", "expression": "o_custkey", "type": "integer"},
                {
                    "name": "orderstatus",
                    "expression": "o_orderstatus",
                    "type": "varchar",
                    "rls": {
                        "name": "SESSION_STATUS",
                        "operator": "EQUALS",
                    },
                },
                {"name": "totalprice", "expression": "o_totalprice", "type": "float"},
                {"name": "orderdate", "expression": "o_orderdate", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "o_orderkey || '_' || o_custkey",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "cast('2024-01-01 23:59:59' as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "cast('2024-01-01 23:59:59 UTC' as timestamp with time zone)",
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "cast(NULL as timestamp)",
                    "type": "timestamp",
                },
            ],
            "primaryKey": "orderkey",
        },
    ],
}

manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


def test_query():
    response = client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        1,
        370,
        "O",
        "172799.49",
        "1996-01-02",
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 UTC",
        None,
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "object",
        "totalprice": "object",
        "orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "object",
        "timestamptz": "object",
        "test_null_time": "datetime64[ns]",
    }

    # TODO: Implement more test
