import pathlib

import pandas as pd
import pytest
import sqlalchemy
from testcontainers.postgres import PostgresContainer

from tests.conftest import file_path

pytestmark = pytest.mark.postgres

base_url = "/v3/connector/postgres"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="module")
def postgres(request) -> PostgresContainer:
    pg = PostgresContainer("postgres:16-alpine").start()
    engine = sqlalchemy.create_engine(pg.get_connection_url())
    pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    pd.read_parquet(file_path("resource/tpch/data/customer.parquet")).to_sql(
        "customer", engine, index=False
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
