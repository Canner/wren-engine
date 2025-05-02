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


@pytest.fixture(scope="module")
def oracle(request) -> OracleDbContainer:
    oracle = OracleDbContainer(
        "gvenzl/oracle-free:23.6-slim-faststart", oracle_password=f"{oracle_password}"
    ).start()
    engine = sqlalchemy.create_engine(oracle.get_connection_url())
    with engine.begin() as conn:
        pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
            "orders", engine, index=False
        )
        pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
            "customer", engine, index=False
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
