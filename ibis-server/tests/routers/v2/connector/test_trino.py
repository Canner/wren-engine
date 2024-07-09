import base64

import orjson
import pytest
from fastapi.testclient import TestClient

from app.main import app
from tests.self_testcontainers.trino import TrinoContainer

pytestmark = pytest.mark.trino

client = TestClient(app)

base_url = "/v2/connector/trino"

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "refSql": "select * from tpch.tiny.orders",
            "columns": [
                {"name": "orderkey", "expression": "orderkey", "type": "integer"},
                {"name": "custkey", "expression": "custkey", "type": "integer"},
                {
                    "name": "orderstatus",
                    "expression": "orderstatus",
                    "type": "varchar",
                },
                {
                    "name": "totalprice",
                    "expression": "totalprice",
                    "type": "float",
                },
                {"name": "orderdate", "expression": "orderdate", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "concat(cast(orderkey as varchar), '_', cast(custkey as varchar))",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "TIMESTAMP '2024-01-01 23:59:59'",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "with_timezone(TIMESTAMP '2024-01-01 23:59:59', 'UTC')",
                    "type": "timestamp",
                },
            ],
            "primaryKey": "orderkey",
        },
    ],
}

manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def trino(request) -> TrinoContainer:
    db = TrinoContainer().start()
    request.addfinalizer(db.stop)
    return db


def test_query(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
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
        172799.49,
        820540800000,
        "1_370",
        1704153599000,
        1704153599000,
    ]
    assert result["dtypes"] == {
        "orderkey": "int64",
        "custkey": "int64",
        "orderstatus": "object",
        "totalprice": "float64",
        "orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "datetime64[ns]",
        "timestamptz": "datetime64[ns, UTC]",
    }


def test_query_with_connection_url(trino: TrinoContainer):
    connection_url = to_connection_url(trino)
    response = client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == len(manifest["models"][0]["columns"])
    assert len(result["data"]) == 1
    assert result["data"][0][0] == 1
    assert result["dtypes"] is not None


def test_query_with_column_dtypes(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" ORDER BY orderkey LIMIT 1',
            "columnDtypes": {
                "totalprice": "float",
                "orderdate": "datetime64",
                "timestamp": "datetime64",
                "timestamptz": "datetime64",
            },
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
        172799.49,
        "1996-01-02 00:00:00.000000",
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 UTC",
    ]
    assert result["dtypes"] == {
        "orderkey": "int64",
        "custkey": "int64",
        "orderstatus": "object",
        "totalprice": "float64",
        "orderdate": "object",
        "order_cust_key": "object",
        "timestamp": "object",
        "timestamptz": "object",
    }


def test_query_with_limit(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/query",
        params={"limit": 1},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders"',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1

    response = client.post(
        url=f"{base_url}/query",
        params={"limit": 1},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 10',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1


def test_query_without_manifest(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "manifestStr"]
    assert result["detail"][0]["msg"] == "Field required"


def test_query_without_sql(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/query",
        json={"connectionInfo": connection_info, "manifestStr": manifest_str},
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "sql"]
    assert result["detail"][0]["msg"] == "Field required"


def test_query_without_connection_info():
    response = client.post(
        url=f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "connectionInfo"]
    assert result["detail"][0]["msg"] == "Field required"


def test_query_with_dry_run(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "Orders" LIMIT 1',
        },
    )
    assert response.status_code == 204


def test_query_with_dry_run_and_invalid_sql(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM X",
        },
    )
    assert response.status_code == 422
    assert response.text is not None


def test_validate_with_unknown_rule(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/validate/unknown_rule",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 422
    assert (
        response.text
        == "The rule `unknown_rule` is not in the rules, rules: ['column_is_valid']"
    )


def test_validate_rule_column_is_valid(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 204


def test_validate_rule_column_is_valid_with_invalid_parameters(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "X", "columnName": "orderkey"},
        },
    )
    assert response.status_code == 422

    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders", "columnName": "X"},
        },
    )
    assert response.status_code == 422


def test_validate_rule_column_is_valid_without_parameters(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={"connectionInfo": connection_info, "manifestStr": manifest_str},
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "parameters"]
    assert result["detail"][0]["msg"] == "Field required"


def test_validate_rule_column_is_valid_without_one_parameter(trino: TrinoContainer):
    connection_info = to_connection_info(trino)
    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"modelName": "Orders"},
        },
    )
    assert response.status_code == 422
    assert response.text == "Missing required parameter: `columnName`"

    response = client.post(
        url=f"{base_url}/validate/column_is_valid",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "parameters": {"columnName": "orderkey"},
        },
    )
    assert response.status_code == 422
    assert response.text == "Missing required parameter: `modelName`"


def to_connection_info(trino: TrinoContainer):
    return {
        "host": trino.get_container_host_ip(),
        "port": trino.get_exposed_port(trino.port),
        "catalog": "tpch",
        "schema": "sf1",
        "user": "test",
    }


def to_connection_url(trino: TrinoContainer):
    info = to_connection_info(trino)
    return f"trino://{info['user']}@{info['host']}:{info['port']}/{info['catalog']}/{info['schema']}"
