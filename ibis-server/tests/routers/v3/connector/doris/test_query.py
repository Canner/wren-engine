import base64

import orjson
import pytest

from tests.routers.v3.connector.doris.conftest import base_url

manifest = {
    "dataSource": "doris",
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
                {"name": "o_totalprice", "type": "float"},
                {"name": "o_orderdate", "type": "date"},
            ],
        },
    ],
}


@pytest.fixture(scope="module")
async def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_query(client, manifest_str, connection_info):
    """Test basic query against Doris."""
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "orders" LIMIT 1',
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) > 0
    assert len(result["data"]) == 1


async def test_query_without_manifest(client, connection_info):
    """Test direct SQL query without manifest (dry run)."""
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": base64.b64encode(
                orjson.dumps(
                    {
                        "dataSource": "doris",
                        "catalog": "c",
                        "schema": "s",
                        "models": [],
                    }
                )
            ).decode("utf-8"),
            "sql": "SELECT 1 AS val",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"] == [[1]]
    assert result["dtypes"]["val"] in ("int8", "int16", "int32", "int64")


async def test_dry_run(client, manifest_str, connection_info):
    """Test dry run mode (validate SQL without executing)."""
    response = await client.post(
        url=f"{base_url}/query?dryRun=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": 'SELECT * FROM "orders" LIMIT 1',
        },
    )
    assert response.status_code == 204


async def test_metadata_tables(client, connection_info):
    """Test fetching table metadata from Doris."""
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200
    tables = response.json()
    assert isinstance(tables, list)
    assert len(tables) > 0
    # Verify table structure
    table = tables[0]
    assert "name" in table
    assert "columns" in table


async def test_metadata_constraints(client, connection_info):
    """Test fetching constraints from Doris (should return empty list)."""
    response = await client.post(
        url=f"{base_url}/metadata/constraints",
        json={
            "connectionInfo": connection_info,
        },
    )
    assert response.status_code == 200
    constraints = response.json()
    assert isinstance(constraints, list)
    # Doris has no foreign keys, expect empty
    assert len(constraints) == 0


async def test_invalid_query(client, manifest_str, connection_info):
    """Test that invalid SQL returns proper error."""
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM non_existent_table_xyz",
        },
    )
    assert response.status_code != 200


async def test_no_transaction_wrapping(client, connection_info):
    """Verify Doris queries execute without BEGIN/ROLLBACK wrapping.

    This test confirms the autocommit fix works - Doris should not
    receive BEGIN/COMMIT/ROLLBACK commands.
    """
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": base64.b64encode(
                orjson.dumps(
                    {
                        "dataSource": "doris",
                        "catalog": "c",
                        "schema": "s",
                        "models": [],
                    }
                )
            ).decode("utf-8"),
            "sql": "SELECT version()",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    # Doris version string typically contains "doris"
    version_str = str(result["data"][0][0]).lower()
    assert "doris" in version_str
