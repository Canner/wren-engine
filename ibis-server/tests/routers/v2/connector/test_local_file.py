import base64

import orjson
import pytest

pytestmark = pytest.mark.local_file


base_url = "/v2/connector/local_file"
manifest = {
    "catalog": "my_calalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "tableReference": {
                "table": "tests/resource/tpch/data/orders.parquet",
            },
            "columns": [
                {"name": "orderkey", "expression": "o_orderkey", "type": "integer"},
                {"name": "custkey", "expression": "o_custkey", "type": "integer"},
                {
                    "name": "orderstatus",
                    "expression": "o_orderstatus",
                    "type": "varchar",
                },
                {
                    "name": "totalprice",
                    "expression": "o_totalprice",
                    "type": "float",
                },
                {"name": "orderdate", "expression": "o_orderdate", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "concat(o_orderkey, '_', o_custkey)",
                    "type": "varchar",
                },
            ],
            "primaryKey": "orderkey",
        },
        {
            "name": "Customer",
            "tableReference": {
                "table": "tests/resource/tpch/data/customer.parquet",
            },
            "columns": [
                {
                    "name": "custkey",
                    "type": "integer",
                    "expression": "c_custkey",
                },
                {
                    "name": "orders",
                    "type": "Orders",
                    "relationship": "CustomerOrders",
                },
                {
                    "name": "sum_totalprice",
                    "type": "float",
                    "isCalculated": True,
                    "expression": "sum(orders.totalprice)",
                },
            ],
            "primaryKey": "custkey",
        },
    ],
    "relationships": [
        {
            "name": "CustomerOrders",
            "models": ["Customer", "Orders"],
            "joinType": "ONE_TO_MANY",
            "condition": "Customer.custkey = Orders.custkey",
        }
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def connection_info() -> dict[str, str]:
    return {
        "url": "tests/resource/tpch/data",
        "format": "parquet",
    }


async def test_query(client, manifest_str, connection_info):
    response = await client.post(
        f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
            "connectionInfo": connection_info,
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
        "1996-01-02 00:00:00.000000",
        "1_370",
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "object",
        "totalprice": "float64",
        "orderdate": "object",
        "order_cust_key": "object",
    }


async def test_query_with_limit(client, manifest_str, connection_info):
    response = await client.post(
        f"{base_url}/query",
        params={"limit": 1},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" limit 2',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1


async def test_query_calculated_field(client, manifest_str, connection_info):
    response = await client.post(
        f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT custkey, sum_totalprice FROM "Customer" WHERE custkey = 370',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 2
    assert len(result["data"]) == 1
    assert result["data"][0] == [
        370,
        "2860895.79",
    ]
    assert result["dtypes"] == {
        "custkey": "int32",
        "sum_totalprice": "float64",
    }


async def test_dry_run(client, manifest_str, connection_info):
    response = await client.post(
        f"{base_url}/query",
        params={"dryRun": True},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 204

    response = await client.post(
        f"{base_url}/query",
        params={"dryRun": True},
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "NotFound" LIMIT 1',
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 422
    assert response.text is not None


async def test_metadata_list_tables(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200

    result = next(filter(lambda x: x["name"] == "orders", response.json()))
    assert result["name"] == "orders"
    assert result["primaryKey"] is None
    assert result["description"] is None
    assert result["properties"] == {
        "catalog": None,
        "schema": None,
        "table": "orders",
        "path": "tests/resource/tpch/data/orders.parquet",
    }
    assert len(result["columns"]) == 9
    assert result["columns"][8] == {
        "name": "o_comment",
        "nestedColumns": None,
        "type": "STRING",
        "notNull": False,
        "description": None,
        "properties": None,
    }


async def test_metadata_list_constraints(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200


async def test_metadata_db_version(client, connection_info):
    response = await client.post(
        url=f"{base_url}/metadata/version",
        json={
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200
    assert "Local File System" in response.text


async def test_unsupported_format(client):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": {
                "url": "tests/resource/tpch/data",
                "format": "unsupported",
            },
        },
    )
    assert response.status_code == 501
    assert response.text == "Unsupported format: unsupported"
