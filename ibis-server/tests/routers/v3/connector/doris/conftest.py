import pathlib
import time

import pandas as pd
import pymysql
import pytest

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.doris

base_url = "/v3/connector/doris"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


function_list_path = file_path("../resources/function_list")


DORIS_HOST = "127.0.0.1"
DORIS_PORT = 9030
DORIS_USER = "root"
DORIS_PASSWORD = ""
DORIS_DATABASE = "wren_test"


@pytest.fixture(scope="session")
def doris(request):
    """Connect to Doris and load TPC-H test data."""
    conn = pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        autocommit=True,
    )
    cursor = conn.cursor()

    # Create test database
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DORIS_DATABASE}`")
    cursor.execute(f"USE `{DORIS_DATABASE}`")

    # Ensure clean state
    cursor.execute("DROP TABLE IF EXISTS orders")

    # Create orders table with Doris DDL
    cursor.execute(
        """
        CREATE TABLE orders (
            o_orderkey      INT,
            o_custkey       INT,
            o_orderstatus   VARCHAR(1),
            o_totalprice    DECIMAL(15, 2),
            o_orderdate     DATE,
            o_orderpriority VARCHAR(15),
            o_clerk         VARCHAR(15),
            o_shippriority  INT,
            o_comment       VARCHAR(79)
        )
        DISTRIBUTED BY HASH(o_orderkey) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
    )

    # Load test data from parquet
    orders_pdf = pd.read_parquet(file_path("resource/tpch/data/orders.parquet"))

    # Convert date column to string for pymysql INSERT
    if "o_orderdate" in orders_pdf.columns:
        orders_pdf["o_orderdate"] = orders_pdf["o_orderdate"].astype(str)

    # Handle NaN values
    orders_pdf = orders_pdf.where(orders_pdf.notna(), None)

    # Batch insert
    columns = list(orders_pdf.columns)
    col_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO orders ({col_str}) VALUES ({placeholders})"

    data = [tuple(row) for row in orders_pdf.values]

    batch_size = 500
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        cursor.executemany(insert_sql, batch)

    cursor.close()
    conn.close()

    # Wait for Doris to make data visible
    time.sleep(3)

    def cleanup():
        try:
            c = pymysql.connect(
                host=DORIS_HOST,
                port=DORIS_PORT,
                user=DORIS_USER,
                password=DORIS_PASSWORD,
                database=DORIS_DATABASE,
                autocommit=True,
            )
            cur = c.cursor()
            cur.execute("DROP TABLE IF EXISTS orders")
            cur.close()
            c.close()
        except Exception:
            pass

    request.addfinalizer(cleanup)


@pytest.fixture(scope="module")
def connection_info(doris) -> dict[str, str]:
    return {
        "host": DORIS_HOST,
        "port": str(DORIS_PORT),
        "user": DORIS_USER,
        "password": DORIS_PASSWORD,
        "database": DORIS_DATABASE,
    }


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    yield
    config.set_remote_function_list_path(None)
