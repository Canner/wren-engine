import base64

import orjson
import pytest

from app.dependencies import X_WREN_FALLBACK_DISABLE, X_WREN_VARIABLE_PREFIX
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
                    "name": "o_orderstatus",
                    "type": "varchar",
                },
                {
                    "name": "o_totalprice",
                    "type": "text",
                    "isHidden": True,
                },
                {
                    "name": "o_totalprice_double",
                    "type": "double",
                    "expression": "cast(o_totalprice as double)",
                },
                {"name": "o_orderdate", "type": "date"},
                {
                    "name": "order_cust_key",
                    "expression": "concat(o_orderkey, '_', o_custkey)",
                    "type": "varchar",
                },
                {
                    "name": "timestamp",
                    "expression": "cast('2024-01-01T23:59:59' as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "cast('2024-01-01T23:59:59' as timestamp with time zone)",
                    "type": "timestamptz",
                },
                {
                    "name": "dst_utc_minus_5",
                    "expression": "cast('2024-01-15 23:00:00 America/New_York' as timestamp with time zone)",
                    "type": "timestamptz",
                },
                {
                    "name": "dst_utc_minus_4",
                    "expression": "cast('2024-07-15 23:00:00 America/New_York' as timestamp with time zone)",
                    "type": "timestamptz",
                },
            ],
            "primaryKey": "o_orderkey",
        },
        {
            "name": "customer",
            "tableReference": {
                "schema": "public",
                "table": "customer",
            },
            "columns": [
                {"name": "c_custkey", "type": "integer"},
                {
                    "name": "c_name",
                    "type": "varchar",
                    "columnLevelAccessControl": {
                        "name": "c_name_access",
                        "requiredProperties": [
                            {
                                "name": "session_level",
                                "required": False,
                            }
                        ],
                        "operator": "EQUALS",
                        "threshold": "1",
                    },
                },
                {"name": "orders", "type": "orders", "relationship": "orders_customer"},
                {
                    "name": "sum_totalprice",
                    "type": "double",
                    "isCalculated": True,
                    "expression": "sum(orders.o_totalprice_double)",
                },
            ],
            "rowLevelAccessControls": [
                {
                    "name": "customer_access",
                    "requiredProperties": [
                        {
                            "name": "session_user",
                            "required": False,
                        }
                    ],
                    "condition": "c_name = @session_user",
                },
            ],
            "primaryKey": "c_custkey",
        },
    ],
    "relationships": [
        {
            "name": "orders_customer",
            "models": ["orders", "customer"],
            "joinType": "many_to_one",
            "condition": "orders.o_custkey = customer.c_custkey",
        }
    ],
}


@pytest.fixture(scope="module")
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


async def test_query(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 10
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
        "2024-01-16 04:00:00.000000 UTC",  # utc-5
        "2024-07-16 03:00:00.000000 UTC",  # utc-4
    ]
    assert result["dtypes"] == {
        "o_orderkey": "int32",
        "o_custkey": "int32",
        "o_orderstatus": "string",
        "o_totalprice_double": "double",
        "o_orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[us]",
        "timestamptz": "timestamp[us, tz=UTC]",
        "dst_utc_minus_5": "timestamp[us, tz=UTC]",
        "dst_utc_minus_4": "timestamp[us, tz=UTC]",
    }

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 0",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 10
    assert len(result["data"]) == 0
    assert result["dtypes"] == {
        "o_orderkey": "int32",
        "o_custkey": "int32",
        "o_orderstatus": "object",
        "o_totalprice_double": "float64",
        "o_orderdate": "datetime64[s]",
        "order_cust_key": "object",
        "timestamp": "datetime64[ns]",
        "timestamptz": "datetime64[ns, UTC]",
        "dst_utc_minus_5": "datetime64[ns, UTC]",
        "dst_utc_minus_4": "datetime64[ns, UTC]",
    }


async def test_query_with_cache(client, manifest_str, connection_info):
    # First request - should miss cache
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )

    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()

    # Second request with same SQL - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )

    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"
    assert int(response2.headers["X-Cache-Create-At"]) > 1743984000  # 2025.04.07
    result2 = response2.json()

    assert result1["data"] == result2["data"]
    assert result1["columns"] == result2["columns"]
    assert result1["dtypes"] == result2["dtypes"]


async def test_query_with_cache_override(client, manifest_str, connection_info):
    # First request - should miss cache then create cache
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",  # Enable cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response1.status_code == 200

    # Second request with same SQL - should hit cache and override it
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true&overrideCache=true",  # Override the cache
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Override"] == "true"
    assert int(response2.headers["X-Cache-Override-At"]) > int(
        response2.headers["X-Cache-Create-At"]
    )


async def test_query_with_connection_url(client, manifest_str, connection_url):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 10
    assert len(result["data"]) == 1
    assert result["data"][0][0] == 1
    assert result["dtypes"] is not None


async def test_query_with_connection_url_and_cache_enable(
    client, manifest_str, connection_url
):
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()

    # Second request with same SQL - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"
    result2 = response2.json()

    assert result1["data"] == result2["data"]
    assert result1["columns"] == result2["columns"]
    assert result1["dtypes"] == result2["dtypes"]


async def test_query_with_connection_url_and_cache_override(
    client, manifest_str, connection_url
):
    # First request - should miss cache then create cache
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response1.status_code == 200

    # Second request with same SQL - should hit cache and override it
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true&overrideCache=true",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Override"] == "true"
    assert int(response2.headers["X-Cache-Override-At"]) > int(
        response2.headers["X-Cache-Create-At"]
    )


async def test_query_with_limit(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        params={"limit": 1},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1

    response = await client.post(
        url=f"{base_url}/query",
        params={"limit": 1},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 10",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1


async def test_query_with_invalid_manifest_str(client, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": "xxx",
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 422


async def test_query_without_manifest(client, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "manifestStr"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_without_sql(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={"connectionInfo": connection_info, "manifestStr": manifest_str},
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "sql"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_without_connection_info(client, manifest_str):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 422
    result = response.json()
    assert result["detail"][0] is not None
    assert result["detail"][0]["type"] == "missing"
    assert result["detail"][0]["loc"] == ["body", "connectionInfo"]
    assert result["detail"][0]["msg"] == "Field required"


async def test_query_with_dry_run(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        params={"dryRun": True},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM wren.public.orders LIMIT 1",
        },
    )
    assert response.status_code == 204


async def test_query_with_dry_run_and_invalid_sql(
    client, manifest_str, connection_info
):
    response = await client.post(
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


async def test_query_to_many_calculation(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT sum_totalprice FROM wren.public.customer limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1
    assert result["dtypes"] == {"sum_totalprice": "double"}

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT sum_totalprice FROM wren.public.customer where c_name = 'Customer#000000001' limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 1
    assert len(result["data"]) == 1
    assert result["dtypes"] == {"sum_totalprice": "double"}

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name, sum_totalprice FROM wren.public.customer limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 2
    assert len(result["data"]) == 1
    assert result["dtypes"] == {"c_name": "string", "sum_totalprice": "double"}

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_custkey, sum_totalprice FROM wren.public.customer limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["columns"]) == 2
    assert len(result["data"]) == 1
    assert result["dtypes"] == {"c_custkey": "int32", "sum_totalprice": "double"}


@pytest.mark.skip(reason="Datafusion does not implement filter yet")
async def test_query_with_keyword_filter(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT count(*) FILTER(WHERE o_orderkey != NULL) FROM wren.public.orders",
        },
    )
    assert response.status_code == 200
    assert response.text is not None


async def test_limit_pushdown(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        params={"limit": 10},
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 100",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 10


async def test_rlac_query(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name FROM customer",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000001'",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert result["data"][0][0] == "Customer#000000001"

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name FROM customer",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "SESSION_USER": "'Customer#000000001'",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert result["data"][0][0] == "Customer#000000001"


async def test_clac_query(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer limit 1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert len(result["data"][0]) == 3

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer limit 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert len(result["data"][0]) == 3

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer limit 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "2",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert len(result["data"][0]) == 2


async def test_format_floating(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
SELECT
    0.0123e-5 AS case_scientific_original,
    1.23e+4 AS case_scientific_positive,
    -4.56e-3 AS case_scientific_negative,
    7.89e0 AS case_scientific_zero_exponent,
    0e0 AS case_scientific_zero,

    123.456 AS case_decimal_positive,
    -123.456 AS case_decimal_negative,
    0.0000123 AS case_decimal_small,
    123.0000 AS case_decimal_trailing_zeros,
    0.0 AS case_decimal_zero,

    0 AS case_integer_zero,
    0e-9 AS case_integer_zero_scientific,
    -1 AS case_integer_negative,
    9999999999 AS case_integer_large,

    1.23e4 + 4.56 AS case_mixed_addition,
    -1.23e-4 - 123.45 AS case_mixed_subtraction,
    0.0123e-5 * 1000 AS case_mixed_multiplication,
    123.45 / 1.23e2 AS case_mixed_division,

    CAST('NaN' AS FLOAT) AS case_special_nan,
    CAST('Infinity' AS FLOAT) AS case_special_infinity,
    CAST('-Infinity' AS FLOAT) AS case_special_negative_infinity,
    NULL AS case_special_null,

    CAST(123.456 AS FLOAT) AS case_cast_float,
    CAST(1.23e4 AS DECIMAL(10,5)) AS case_cast_decimal,
    CAST(1.234e+14 AS DECIMAL(20,0)) AS show_float,
    CAST(1.234e+15 AS DECIMAL(20,0)) AS show_exponent,
    CAST(1.123456789 AS DECIMAL(20,9)) AS round_to_9_decimal_places,
    CAST(0.123456789123456789 AS DECIMAL(20,18)) AS round_to_18_decimal_places
            """,
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",  # Disable fallback to DuckDB
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"][0][0] == "0.00000012"
    assert result["data"][0][1] == "12300"
    assert result["data"][0][2] == "-0.00456"
    assert result["data"][0][3] == "7.89"
    assert result["data"][0][4] == "0"
    assert result["data"][0][5] == "123.456"
    assert result["data"][0][6] == "-123.456"
    assert result["data"][0][7] == "0.0000123"
    assert result["data"][0][8] == "123"
    assert result["data"][0][9] == "0"
    assert result["data"][0][10] == 0
    assert result["data"][0][11] == "0"
    assert result["data"][0][12] == -1
    assert result["data"][0][13] == 9999999999
    assert result["data"][0][14] == "12304.56"
    assert result["data"][0][15] == "-123.450123"
    assert result["data"][0][16] == "0.000123"
    assert result["data"][0][17] == "1.00365854"
    assert result["data"][0][18] is None
    assert result["data"][0][19] == "inf"
    assert result["data"][0][20] == "-inf"
    assert result["data"][0][21] is None
    assert result["data"][0][22] == "123.45600128"
    assert result["data"][0][23] == "12300"
    assert result["data"][0][24] == "123400000000000"
    assert result["data"][0][25] == "1.234e+15"
    assert result["data"][0][26] == "1.12345679"
    assert result["data"][0][27] == "0.123456789"
