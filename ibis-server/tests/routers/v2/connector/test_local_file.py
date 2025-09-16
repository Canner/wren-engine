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
    "dataSource": "local_file",
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
        "1996-01-02",
        "1_370",
    ]
    assert result["dtypes"] == {
        "orderkey": "int32",
        "custkey": "int32",
        "orderstatus": "string",
        "totalprice": "decimal128(15, 2)",
        "orderdate": "date32[day]",
        "order_cust_key": "string",
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
        "sum_totalprice": "decimal128(38, 2)",
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
    assert response.status_code == 422
    assert (
        response.json()["message"]
        == "Failed to list files: Unsupported format: unsupported"
    )


async def test_list_parquet_files(client):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": {
                "url": "tests/resource/test_file_source",
                "format": "parquet",
            },
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result) == 2
    table_names = [table["name"] for table in result]
    assert "type-test-parquet" in table_names
    assert "type-test" in table_names
    columns = result[0]["columns"]
    assert len(columns) == 23
    assert columns[0]["name"] == "c_bigint"
    assert columns[0]["type"] == "INT64"
    assert columns[1]["name"] == "c_bit"
    assert columns[1]["type"] == "STRING"
    assert columns[2]["name"] == "c_blob"
    assert columns[2]["type"] == "BYTES"
    assert columns[3]["name"] == "c_boolean"
    assert columns[3]["type"] == "BOOL"
    assert columns[4]["name"] == "c_date"
    assert columns[4]["type"] == "DATE"
    assert columns[5]["name"] == "c_double"
    assert columns[5]["type"] == "DOUBLE"
    assert columns[6]["name"] == "c_float"
    assert columns[6]["type"] == "FLOAT"
    assert columns[7]["name"] == "c_integer"
    assert columns[7]["type"] == "INT"
    assert columns[8]["name"] == "c_hugeint"
    assert columns[8]["type"] == "DOUBLE"
    assert columns[9]["name"] == "c_interval"
    assert columns[9]["type"] == "INTERVAL"
    assert columns[10]["name"] == "c_json"
    assert columns[10]["type"] == "JSON"
    assert columns[11]["name"] == "c_smallint"
    assert columns[11]["type"] == "INT2"
    assert columns[12]["name"] == "c_time"
    assert columns[12]["type"] == "TIME"
    assert columns[13]["name"] == "c_timestamp"
    assert columns[13]["type"] == "TIMESTAMP"
    assert columns[14]["name"] == "c_timestamptz"
    assert columns[14]["type"] == "TIMESTAMPTZ"
    assert columns[15]["name"] == "c_tinyint"
    assert columns[15]["type"] == "INT2"
    assert columns[16]["name"] == "c_ubigint"
    assert columns[16]["type"] == "INT64"
    assert columns[17]["name"] == "c_uhugeint"
    assert columns[17]["type"] == "DOUBLE"
    assert columns[18]["name"] == "c_uinteger"
    assert columns[18]["type"] == "INT"
    assert columns[19]["name"] == "c_usmallint"
    assert columns[19]["type"] == "INT2"
    assert columns[20]["name"] == "c_utinyint"
    assert columns[20]["type"] == "INT2"
    assert columns[21]["name"] == "c_uuid"
    assert columns[21]["type"] == "UUID"
    assert columns[22]["name"] == "c_varchar"
    assert columns[22]["type"] == "STRING"


async def test_list_csv_files(client):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": {
                "url": "tests/resource/test_file_source",
                "format": "csv",
            },
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result) == 3
    table_names = [table["name"] for table in result]
    assert "type-test-csv" in table_names
    assert "type-test" in table_names
    # `invalid` will be considered as a one column csv file
    assert "invalid" in table_names
    columns = next(filter(lambda x: x["name"] == "type-test-csv", result))["columns"]
    assert columns[0]["name"] == "c_bigint"
    assert columns[0]["type"] == "INT64"
    assert columns[1]["name"] == "c_bit"
    assert columns[1]["type"] == "STRING"
    assert columns[2]["name"] == "c_blob"
    assert columns[2]["type"] == "STRING"
    assert columns[3]["name"] == "c_boolean"
    assert columns[3]["type"] == "BOOL"
    assert columns[4]["name"] == "c_date"
    assert columns[4]["type"] == "DATE"
    assert columns[5]["name"] == "c_double"
    assert columns[5]["type"] == "DOUBLE"
    assert columns[6]["name"] == "c_float"
    assert columns[6]["type"] == "DOUBLE"
    assert columns[7]["name"] == "c_integer"
    assert columns[7]["type"] == "INT64"
    assert columns[8]["name"] == "c_hugeint"
    assert columns[8]["type"] == "INT64"
    assert columns[9]["name"] == "c_interval"
    assert columns[9]["type"] == "STRING"
    assert columns[10]["name"] == "c_json"
    assert columns[10]["type"] == "STRING"
    assert columns[11]["name"] == "c_smallint"
    assert columns[11]["type"] == "INT64"
    assert columns[12]["name"] == "c_time"
    assert columns[12]["type"] == "TIME"
    assert columns[13]["name"] == "c_timestamp"
    assert columns[13]["type"] == "TIMESTAMP"
    assert columns[14]["name"] == "c_timestamptz"
    assert columns[14]["type"] == "TIMESTAMPTZ"
    assert columns[15]["name"] == "c_tinyint"
    assert columns[15]["type"] == "INT64"
    assert columns[16]["name"] == "c_ubigint"
    assert columns[16]["type"] == "INT64"
    assert columns[17]["name"] == "c_uhugeint"
    assert columns[17]["type"] == "INT64"
    assert columns[18]["name"] == "c_uinteger"
    assert columns[18]["type"] == "INT64"
    assert columns[19]["name"] == "c_usmallint"
    assert columns[19]["type"] == "INT64"
    assert columns[20]["name"] == "c_utinyint"
    assert columns[20]["type"] == "INT64"
    assert columns[21]["name"] == "c_uuid"
    assert columns[21]["type"] == "STRING"
    assert columns[22]["name"] == "c_varchar"
    assert columns[22]["type"] == "STRING"


async def test_list_json_files(client):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": {
                "url": "tests/resource/test_file_source",
                "format": "json",
            },
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result) == 2
    table_names = [table["name"] for table in result]
    assert "type-test-json" in table_names
    assert "type-test" in table_names

    columns = result[0]["columns"]
    assert columns[0]["name"] == "c_bigint"
    assert columns[0]["type"] == "INT64"
    assert columns[1]["name"] == "c_bit"
    assert columns[1]["type"] == "STRING"
    assert columns[2]["name"] == "c_blob"
    assert columns[2]["type"] == "STRING"
    assert columns[3]["name"] == "c_boolean"
    assert columns[3]["type"] == "BOOL"
    assert columns[4]["name"] == "c_date"
    assert columns[4]["type"] == "DATE"
    assert columns[5]["name"] == "c_double"
    assert columns[5]["type"] == "DOUBLE"
    assert columns[6]["name"] == "c_float"
    assert columns[6]["type"] == "DOUBLE"
    assert columns[7]["name"] == "c_integer"
    assert columns[7]["type"] == "INT64"
    assert columns[8]["name"] == "c_hugeint"
    assert columns[8]["type"] == "DOUBLE"
    assert columns[9]["name"] == "c_interval"
    assert columns[9]["type"] == "STRING"
    assert columns[10]["name"] == "c_json"
    assert columns[10]["type"] == "UNKNOWN"
    assert columns[11]["name"] == "c_smallint"
    assert columns[11]["type"] == "INT64"
    assert columns[12]["name"] == "c_time"
    assert columns[12]["type"] == "TIME"
    assert columns[13]["name"] == "c_timestamp"
    assert columns[13]["type"] == "TIMESTAMP"
    assert columns[14]["name"] == "c_timestamptz"
    assert columns[14]["type"] == "STRING"
    assert columns[15]["name"] == "c_tinyint"
    assert columns[15]["type"] == "INT64"
    assert columns[16]["name"] == "c_ubigint"
    assert columns[16]["type"] == "INT64"
    assert columns[17]["name"] == "c_uhugeint"
    assert columns[17]["type"] == "DOUBLE"
    assert columns[18]["name"] == "c_uinteger"
    assert columns[18]["type"] == "INT64"
    assert columns[19]["name"] == "c_usmallint"
    assert columns[19]["type"] == "INT64"
    assert columns[20]["name"] == "c_utinyint"
    assert columns[20]["type"] == "INT64"
    assert columns[21]["name"] == "c_uuid"
    assert columns[21]["type"] == "UUID"
    assert columns[22]["name"] == "c_varchar"
    assert columns[22]["type"] == "STRING"


async def test_duckdb_metadata_list_tables(client):
    response = await client.post(
        url=f"{base_url}/metadata/tables",
        json={
            "connectionInfo": {
                "url": "tests/resource/test_file_source",
                "format": "duckdb",
            },
        },
    )
    assert response.status_code == 200

    result = next(filter(lambda x: x["name"] == "main.customers", response.json()))
    assert result["name"] == "main.customers"
    assert result["primaryKey"] is not None
    assert result["description"] is None
    assert result["properties"] == {
        "catalog": "jaffle_shop",
        "schema": "main",
        "table": "customers",
        "path": None,
    }
    assert len(result["columns"]) == 7
    assert result["columns"][1] == {
        "name": "number_of_orders",
        "nestedColumns": None,
        "type": "INT64",
        "notNull": False,
        "description": None,
        "properties": None,
    }
