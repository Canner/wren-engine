import pathlib

import pytest
from testcontainers.mssql import SqlServerContainer

pytestmark = pytest.mark.mssql

base_url = "/v3/connector/mssql"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="module")
def mssql(request) -> SqlServerContainer:
    mssql = SqlServerContainer(
        "mcr.microsoft.com/mssql/server:2019-CU27-ubuntu-20.04", dialect="mssql+pyodbc"
    ).start()
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
