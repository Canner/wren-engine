import base64

import orjson
import pytest

from app.dependencies import X_WREN_FALLBACK_DISABLE, X_WREN_VARIABLE_PREFIX
from app.model.data_source import X_WREN_DB_STATEMENT_TIMEOUT
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
                                # To test the name is case insensitive
                                "name": "Session_level",
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
                            # To test the name is case insensitive
                            "name": "Session_user",
                            "required": False,
                        }
                    ],
                    "condition": "c_name = @session_User",
                },
            ],
            "primaryKey": "c_custkey",
        },
        {
            "name": "null_test",
            "tableReference": {
                "schema": "public",
                "table": "null_test",
            },
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "letter", "type": "varchar"},
            ],
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
        172799.49,
        "1996-01-02",
        "1_370",
        "2024-01-01 23:59:59.000000",
        "2024-01-01 23:59:59.000000 +00:00",
        "2024-01-16 04:00:00.000000 +00:00",  # utc-5
        "2024-07-16 03:00:00.000000 +00:00",  # utc-4
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
        "o_orderstatus": "string",
        "o_totalprice_double": "double",
        "o_orderdate": "date32[day]",
        "order_cust_key": "string",
        "timestamp": "timestamp[us]",
        "timestamptz": "timestamp[us, tz=UTC]",
        "dst_utc_minus_5": "timestamp[us, tz=UTC]",
        "dst_utc_minus_4": "timestamp[us, tz=UTC]",
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

    manifest_with_required_properties = {
        "catalog": "wren",
        "schema": "public",
        "models": [
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
                                    "name": "Session_level",
                                    "required": True,
                                }
                            ],
                            "operator": "EQUALS",
                            "threshold": "1",
                        },
                    },
                ],
                "primaryKey": "c_custkey",
            },
        ],
    }

    base64_manifest_with_required_properties = base64.b64encode(
        orjson.dumps(manifest_with_required_properties)
    ).decode("utf-8")
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": base64_manifest_with_required_properties,
            "sql": "SELECT * FROM customer limit 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert len(result["data"][0]) == 2

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": base64_manifest_with_required_properties,
            "sql": "SELECT * FROM customer limit 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "2",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert len(result["data"][0]) == 1


async def test_connection_timeout(
    client, manifest_str, connection_info, connection_url
):
    # Set a very short timeout to force a timeout error
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT 1 FROM (SELECT pg_sleep(5))",  # This will take longer than the default timeout
        },
        headers={X_WREN_DB_STATEMENT_TIMEOUT: "1"},  # Set timeout to 1 second
    )
    assert (
        "Query was cancelled: canceling statement due to statement timeout"
        in response.text
    )
    assert response.status_code == 504

    # test connection_url way can also timeout
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": {"connectionUrl": connection_url},
            "manifestStr": manifest_str,
            "sql": "SELECT 1 FROM (SELECT pg_sleep(5))",  # This will take longer than the default timeout
        },
        headers={X_WREN_DB_STATEMENT_TIMEOUT: "1"},  # Set timeout to 1 second
    )
    assert (
        "Query was cancelled: canceling statement due to statement timeout"
        in response.text
    )
    assert response.status_code == 504


async def test_format_floating(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": """
SELECT
    0.0123e-3 AS case_scientific_original,
    1.23e+4 AS case_scientific_positive,
    -4.56e-3 AS case_scientific_negative,
    7.89e0 AS case_scientific_zero_exponent,
    0e0 AS case_scientific_zero,

    123.456 AS case_decimal_positive,
    -123.456 AS case_decimal_negative,
    0.00000123 AS show_exponent_decimal,
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
    CAST(1.123456789 AS DECIMAL(20,9)) AS round_to_9_decimal_places
            """,
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",  # Disable fallback to DuckDB
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"][0] == [
        "0.0000123",
        "12300.0",
        "-0.00456",
        "7.89",
        "0",
        "123.456",
        "-123.456",
        "1.23e-6",
        "123.0",
        "0",
        0,
        "0",
        -1,
        9999999999,
        "12304.56",
        "-123.450123",
        "0.000123",
        1.0036585365853659,
        None,  # NaN
        None,  # Infinity
        None,  # Negative Infinity
        None,  # NULL
        123.45600128173828,  # case_cast_float
        "12300.0",  # case_cast_decimal
        "123400000000000.0",  # show_float
        # TODO: it's better to show exponent in scientific notation but currently
        # DataFusion does not support it, so we show the full number
        "1234000000000000.0",  # show_exponent
        "1.123456789",  # round_to_9_decimal_places
    ]


async def test_decimal_precision(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT cast(1 as decimal(38, 8)) / cast(3 as decimal(38, 8)) as result",
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 1
    assert result["data"][0][0] == "0.333333333"


async def test_order_by_nulls_last(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT letter FROM null_test ORDER BY id",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",  # Disable fallback to DuckDB
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 3
    assert result["data"][0][0] == "one"
    assert result["data"][1][0] == "two"
    assert result["data"][2][0] == "three"

    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT letter FROM null_test ORDER BY id desc",
        },
        headers={
            X_WREN_FALLBACK_DISABLE: "true",  # Disable fallback to DuckDB
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["data"]) == 3
    assert result["data"][0][0] == "two"
    assert result["data"][1][0] == "one"
    assert result["data"][2][0] == "three"


async def test_cache_with_different_wren_variables(
    client, manifest_str, connection_info
):
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name FROM customer",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000001'",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"  # Cache miss on first request
    result1 = response1.json()
    assert len(result1["data"]) == 1
    assert result1["data"][0][0] == "Customer#000000001"

    # Second request with same session_user - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name FROM customer",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000001'",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"  # Should hit cache
    result2 = response2.json()
    assert result1["data"] == result2["data"]

    # Third request with different session_user - should miss cache and create new entry
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT c_name FROM customer",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000002'",
        },
    )
    assert response3.status_code == 200
    assert (
        response3.headers["X-Cache-Hit"] == "false"
    )  # Should miss cache due to different header
    result3 = response3.json()
    # Results should be different due to row-level access control
    assert result1["data"] != result3["data"] if len(result3["data"]) > 0 else True


async def test_cache_with_different_session_levels(
    client, manifest_str, connection_info
):
    # First request with session_level = 1
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"
    result1 = response1.json()
    assert len(result1["data"][0]) == 3  # Should show c_name column

    # Second request with same session_level - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"
    assert result1["data"] == response2.json()["data"]

    # Third request with different session_level - should miss cache
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "2",
        },
    )
    assert response3.status_code == 200
    assert (
        response3.headers["X-Cache-Hit"] == "false"
    )  # Different header should miss cache
    result3 = response3.json()
    assert len(result3["data"][0]) == 2  # Should hide c_name column


async def test_cache_with_timeout_headers(client, manifest_str, connection_info):
    # First request with timeout = 30
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            X_WREN_DB_STATEMENT_TIMEOUT: "30",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"

    # Second request with same timeout - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            X_WREN_DB_STATEMENT_TIMEOUT: "30",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"

    # Third request with different timeout - should miss cache
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            X_WREN_DB_STATEMENT_TIMEOUT: "60",
        },
    )
    assert response3.status_code == 200
    assert (
        response3.headers["X-Cache-Hit"] == "false"
    )  # Different timeout should miss cache


async def test_cache_ignores_irrelevant_headers(client, manifest_str, connection_info):
    # First request with user-agent header
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            "User-Agent": "TestClient/1.0",
            "X-Request-ID": "request-123",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"

    # Second request with different irrelevant headers - should still hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            "User-Agent": "TestClient/2.0",
            "X-Request-ID": "request-456",
            "Accept": "application/json",
        },
    )
    assert response2.status_code == 200
    assert (
        response2.headers["X-Cache-Hit"] == "true"
    )  # Should hit cache despite different irrelevant headers


async def test_cache_with_multiple_wren_variables(
    client, manifest_str, connection_info
):
    # First request with multiple variables
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000001'",
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"

    # Second request with same variables - should hit cache
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000001'",
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"

    # Third request with one variable changed - should miss cache
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX
            + "session_user": "'Customer#000000002'",  # Changed this value
            X_WREN_VARIABLE_PREFIX + "session_level": "1",
        },
    )
    assert response3.status_code == 200
    assert response3.headers["X-Cache-Hit"] == "false"

    # Fourth request with variables in different order - should still hit cache for first combo
    response4 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM customer LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",  # Different order
            X_WREN_VARIABLE_PREFIX + "session_user": "'Customer#000000001'",
        },
    )
    assert response4.status_code == 200
    assert (
        response4.headers["X-Cache-Hit"] == "true"
    )  # Should hit cache despite different header order


async def test_cache_with_mixed_relevant_irrelevant_headers(
    client, manifest_str, connection_info
):
    # First request
    response1 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",  # Relevant
            "User-Agent": "TestClient/1.0",  # Irrelevant
            X_WREN_DB_STATEMENT_TIMEOUT: "30",  # Relevant
            "X-Request-ID": "abc123",  # Irrelevant
        },
    )
    assert response1.status_code == 200
    assert response1.headers["X-Cache-Hit"] == "false"

    # Second request - change irrelevant headers, keep relevant ones same
    response2 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "1",  # Same
            "User-Agent": "DifferentClient/2.0",  # Changed but irrelevant
            X_WREN_DB_STATEMENT_TIMEOUT: "30",  # Same
            "X-Request-ID": "xyz789",  # Changed but irrelevant
            "Accept": "application/json",  # New but irrelevant
        },
    )
    assert response2.status_code == 200
    assert response2.headers["X-Cache-Hit"] == "true"  # Should hit cache

    # Third request - change one relevant header
    response3 = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT * FROM orders LIMIT 1",
        },
        headers={
            X_WREN_VARIABLE_PREFIX + "session_level": "2",  # Changed (relevant)
            "User-Agent": "TestClient/1.0",  # Back to original
            X_WREN_DB_STATEMENT_TIMEOUT: "30",  # Same
            "X-Request-ID": "abc123",  # Back to original
        },
    )
    assert response3.status_code == 200
    assert (
        response3.headers["X-Cache-Hit"] == "false"
    )  # Should miss cache due to changed relevant header


async def test_query_unicode_table(client, connection_info):
    manifest = {
        "catalog": "wrenai",
        "schema": "public",
        "models": [
            {
                "name": "中文表",
                "tableReference": {"schema": "public", "table": "中文表"},
                "columns": [
                    {"name": "欄位1", "type": "int"},
                    {"name": "欄位2", "type": "int"},
                ],
            }
        ],
        "dataSource": "postgres",
    }

    manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")

    response = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT 欄位1, 欄位2 FROM 中文表 LIMIT 1",
        },
        headers={X_WREN_FALLBACK_DISABLE: "true"},
    )

    assert response.status_code == 200
    result = response.json()
    assert result["data"][0] == [1, 2]
    assert result["dtypes"] == {
        "欄位1": "int32",
        "欄位2": "int32",
    }


async def test_case_sensitive_without_quote(client, connection_info):
    manifest = {
        "catalog": "wren",
        "schema": "public",
        "models": [
            {
                "name": "Orders",
                "tableReference": {
                    "schema": "public",
                    "table": "orders",
                },
                "columns": [
                    {
                        "name": "O_orderkey",
                        "type": "integer",
                        "expression": "o_orderkey",
                    },
                    {"name": "O_custkey", "type": "integer", "expression": "o_custkey"},
                ],
            }
        ],
    }

    manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")

    response = await client.post(
        url=f"{base_url}/query?cacheEnable=true",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT O_orderkey, O_custkey FROM Orders LIMIT 1",
        },
        headers={X_WREN_FALLBACK_DISABLE: "true"},
    )

    assert response.status_code == 200
    result = response.json()
    assert result["dtypes"] == {
        "O_orderkey": "int32",
        "O_custkey": "int32",
    }


async def test_to_char_numeric(client, manifest_str, connection_info):
    response = await client.post(
        url=f"{base_url}/query",
        json={
            "connectionInfo": connection_info,
            "manifestStr": manifest_str,
            "sql": "SELECT to_char(1234, '9999') AS formatted_number, to_char(1234.567, '0000.00') AS formatted_double",
        },
        headers={X_WREN_FALLBACK_DISABLE: "true"},
    )
    assert response.status_code == 200
    result = response.json()
    assert result["data"][0][0] == " 1234"
    assert result["data"][0][1] == " 1234.57"
    assert result["dtypes"] == {
        "formatted_number": "string",
        "formatted_double": "string",
    }
