import pathlib

import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy import text
from testcontainers.mssql import SqlServerContainer

from tests.conftest import file_path

pytestmark = pytest.mark.mssql

base_url = "/v3/connector/mssql"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="session")
def mssql(request) -> SqlServerContainer:
    mssql = SqlServerContainer(
        "mcr.microsoft.com/mssql/server:2019-CU27-ubuntu-20.04", dialect="mssql+pyodbc"
    ).start()
    engine = sqlalchemy.create_engine(
        f"{mssql.get_connection_url()}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=YES"
    )
    pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE unicode_test (id INT, letter NVARCHAR(10))"))
        conn.execute(
            text("INSERT INTO unicode_test (id, letter) VALUES (1, N'真夜中')")
        )
        conn.execute(
            text("INSERT INTO unicode_test (id, letter) VALUES (2, 'ZUTOMAYO')")
        )

    request.addfinalizer(mssql.stop)
    return mssql


@pytest.fixture(scope="module")
def connection_info(mssql: SqlServerContainer) -> dict[str, str]:
    return {
        "host": mssql.get_container_host_ip(),
        "port": mssql.get_exposed_port(mssql.port),
        "user": mssql.username,
        "password": mssql.password,
        "database": mssql.dbname,
        "kwargs": {"TrustServerCertificate": "YES"},
    }
