import base64

import orjson
import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage

from tests.confest import file_path

pytestmark = pytest.mark.druid

base_url = "/v2/connector/druid"

manifest = {
    "catalog": "my_catalog",
    "schema": "my_schema",
    "models": [
        {
            "name": "Orders",
            "refSql": "select * from public.orders",
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
                {
                    "name": "timestamp",
                    "expression": "cast('2024-01-01T23:59:59' as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "timestamptz",
                    "expression": "cast('2024-01-01T23:59:59' as timestamp with time zone)",
                    "type": "timestamp",
                },
                {
                    "name": "test_null_time",
                    "expression": "cast(NULL as timestamp)",
                    "type": "timestamp",
                },
                {
                    "name": "bytea_column",
                    "expression": "cast('abc' as bytea)",
                    "type": "bytea",
                },
            ],
            "primaryKey": "orderkey",
        },
    ],
}


@pytest.fixture
def manifest_str():
    return base64.b64encode(orjson.dumps(manifest)).decode("utf-8")


@pytest.fixture(scope="module")
def docker(request) -> DockerContainer:
    with DockerImage(tag="apache/druid:latest") as image:
        with DockerContainer(str(image)) as container:
            druid = container.start()
            engine = sqlalchemy.create_engine(druid.get_connection_url())
            pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
                "orders", engine, index=False
            )
            with engine.begin() as conn:
                conn.execute(
                    text("COMMENT ON TABLE orders IS 'This is a table comment'")
                )
                conn.execute(
                    text("COMMENT ON COLUMN orders.o_comment IS 'This is a comment'")
                )
            request.addfinalizer(druid.stop)
            return druid
