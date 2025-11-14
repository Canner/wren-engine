import pathlib

import pandas as pd
import pytest
import sqlalchemy
from testcontainers.mysql import MySqlContainer

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.mysql

base_url = "/v3/connector/mysql"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="session")
def mysql(request) -> MySqlContainer:
    mysql = MySqlContainer(image="mysql:8.0.40", dialect="pymysql").start()
    connection_url = mysql.get_connection_url()
    engine = sqlalchemy.create_engine(connection_url)
    pd.read_parquet(file_path("resource/tpch/data/orders.parquet")).to_sql(
        "orders", engine, index=False
    )
    with engine.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                """
                CREATE TABLE json_test (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                object_col JSON NOT NULL,
                array_col  JSON NOT NULL,
                CHECK (JSON_TYPE(object_col) = 'OBJECT'),
                CHECK (JSON_TYPE(array_col)  = 'ARRAY')
                ) ENGINE=InnoDB;
            """
            )
        )
        conn.execute(
            sqlalchemy.text(
                """
                INSERT INTO json_test (object_col, array_col) VALUES
                ('{"name": "Alice", "age": 30, "city": "New York"}', '["apple", "banana", "cherry"]'),
                ('{"name": "Bob", "age": 25, "city": "Los Angeles"}', '["dog", "cat", "mouse"]'),
                ('{"name": "Charlie", "age": 35, "city": "Chicago"}', '["red", "green", "blue"]');
            """
            )
        )
        conn.commit()

    request.addfinalizer(mysql.stop)
    return mysql


function_list_path = file_path("../resources/function_list")
white_function_list_path = file_path("../resources/white_function_list")


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    yield
    config.set_remote_function_list_path(None)


@pytest.fixture(autouse=True)
def set_remote_white_function_list_path():
    config = get_config()
    config.set_remote_white_function_list_path(white_function_list_path)
    yield
    config.set_remote_white_function_list_path(None)


@pytest.fixture(scope="module")
def connection_info(mysql: MySqlContainer) -> dict[str, str]:
    return {
        "host": mysql.get_container_host_ip(),
        "port": mysql.get_exposed_port(mysql.port),
        "user": mysql.username,
        "password": mysql.password,
        "database": mysql.dbname,
    }
