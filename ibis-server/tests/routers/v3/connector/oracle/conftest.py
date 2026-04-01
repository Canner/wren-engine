import pathlib

import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.oracle import OracleDbContainer

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.oracle

base_url = "/v3/connector/oracle"
oracle_password = "Oracle123"
oracle_user = "SYSTEM"
oracle_database = "FREEPDB1"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="session")
def oracle(request) -> OracleDbContainer:
    oracle = OracleDbContainer(
        "gvenzl/oracle-free:23.6-slim-faststart", oracle_password=f"{oracle_password}"
    ).start()
    orders_schema = {
        "o_orderkey": sqlalchemy.Integer(),
        "o_custkey": sqlalchemy.Integer(),
        "o_orderstatus": sqlalchemy.Text(),
        "o_totalprice": sqlalchemy.DECIMAL(precision=38, scale=2),
        "o_orderdate": sqlalchemy.Date(),
        "o_orderpriority": sqlalchemy.Text(),
        "o_clerk": sqlalchemy.Text(),
        "o_shippriority": sqlalchemy.Integer(),
        "o_comment": sqlalchemy.Text(),
    }
    customer_schema = {
        "c_custkey": sqlalchemy.Integer(),
        "c_name": sqlalchemy.Text(),
        "c_address": sqlalchemy.Text(),
        "c_nationkey": sqlalchemy.Integer(),
        "c_phone": sqlalchemy.Text(),
        "c_acctbal": sqlalchemy.DECIMAL(precision=38, scale=2),
        "c_mktsegment": sqlalchemy.Text(),
        "c_comment": sqlalchemy.Text(),
    }
    engine = sqlalchemy.create_engine(oracle.get_connection_url())
    with engine.begin() as conn:
        # assign dtype to avoid to create CLOB column for text columns
        pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
            "orders",
            engine,
            index=False,
            dtype=orders_schema,
        )
        pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
            "customer",
            engine,
            index=False,
            dtype=customer_schema,
        )

        # Create a table with a large CLOB column
        large_text = "x" * (1024 * 1024 * 2)  # 2MB
        conn.execute(text("CREATE TABLE test_lob (id NUMBER, content CLOB)"))
        conn.execute(
            text("INSERT INTO test_lob VALUES (1, :content)"), {"content": large_text}
        )

        # Add table and column comments
        conn.execute(text("COMMENT ON TABLE orders IS 'This is a table comment'"))
        conn.execute(text("COMMENT ON COLUMN orders.o_comment IS 'This is a comment'"))

        # Create a table with NUMBER types
        conn.execute(
            text(
                "CREATE TABLE test_number (id NUMBER, id_p NUMBER(10), id_p_s NUMBER(10, 2))"
            )
        )
        conn.execute(
            text(
                "INSERT INTO test_number (id, id_p, id_p_s) VALUES (1, 1234567890, 12345678.12)"
            )
        )
        conn.execute(text('CREATE TABLE "null_test" ("id" INT, "letter" CLOB)'))
        conn.execute(
            text(
                "INSERT INTO \"null_test\" (\"id\", \"letter\") VALUES (1, 'one'), (2, 'two'), (NULL, 'three')"
            )
        )
    request.addfinalizer(oracle.stop)
    return oracle


@pytest.fixture(scope="module")
def connection_info(oracle: OracleDbContainer):
    # We can't use oracle.user, oracle.password, oracle.dbname here
    # since these values are None at this point
    return {
        "host": oracle.get_container_host_ip(),
        "port": oracle.get_exposed_port(oracle.port),
        "user": f"{oracle_user}",
        "password": f"{oracle_password}",
        "database": f"{oracle_database}",
    }


@pytest.fixture(scope="module")
def connection_url(connection_info: dict[str, str]):
    info = connection_info
    return f"oracle://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"


function_list_path = file_path("../resources/function_list")


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    yield
    config.set_remote_function_list_path(None)
