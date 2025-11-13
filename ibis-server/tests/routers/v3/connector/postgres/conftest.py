import pathlib

import pandas as pd
import pytest
import sqlalchemy
from testcontainers.postgres import PostgresContainer

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.postgres

base_url = "/v3/connector/postgres"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


function_list_path = file_path("../resources/function_list")
white_function_list_path = file_path("../resources/white_function_list")


@pytest.fixture(scope="session")
def postgres(request) -> PostgresContainer:
    pg = PostgresContainer("postgres:16-alpine").start()
    engine = sqlalchemy.create_engine(pg.get_connection_url())
    pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
        "customer", engine, index=False
    )
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE TABLE null_test (id INT, letter TEXT)"))
        conn.execute(
            sqlalchemy.text(
                "INSERT INTO null_test (id, letter) VALUES (1, 'one'), (2, 'two'), (NULL, 'three')"
            )
        )
        conn.execute(sqlalchemy.text("CREATE TABLE 中文表 (欄位1 int, 欄位2 int)"))
        conn.execute(
            sqlalchemy.text("INSERT INTO 中文表 (欄位1, 欄位2) VALUES (1, 2), (3, 4)")
        )

    request.addfinalizer(pg.stop)
    return pg


@pytest.fixture(scope="module")
def connection_info(postgres: PostgresContainer) -> dict[str, str]:
    return {
        "host": postgres.get_container_host_ip(),
        "port": postgres.get_exposed_port(postgres.port),
        "user": postgres.username,
        "password": postgres.password,
        "database": postgres.dbname,
    }


@pytest.fixture(scope="module")
def connection_url(connection_info: dict[str, str]):
    info = connection_info
    return f"postgres://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['database']}"


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    config.set_remote_white_function_list_path(white_function_list_path)
    yield
    config.set_remote_function_list_path(None)
    config.set_remote_white_function_list_path(None)
