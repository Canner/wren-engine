import pathlib

import pytest
from testcontainers.mysql import MySqlContainer

pytestmark = pytest.mark.mysql

base_url = "/v3/connector/mysql"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="module")
def mysql(request) -> MySqlContainer:
    mysql = MySqlContainer(image="mysql:8.0.40", dialect="pymysql").start()
    request.addfinalizer(mysql.stop)
    return mysql


@pytest.fixture(scope="module")
def connection_info(mysql: MySqlContainer) -> dict[str, str]:
    return {
        "host": mysql.get_container_host_ip(),
        "port": mysql.get_exposed_port(mysql.port),
        "user": mysql.username,
        "password": mysql.password,
        "database": mysql.dbname,
    }
