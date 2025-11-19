import os
import pathlib

import pytest
from databricks import sql

pytestmark = pytest.mark.databricks

base_url = "/v3/connector/databricks"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="module", autouse=True)
def init_databricks(connection_info):
    # create schema `wren` and some tables for testing
    try:
        connection = sql.connect(
            server_hostname=connection_info["serverHostname"],
            http_path=connection_info["httpPath"],
            access_token=connection_info["accessToken"],
        )
        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS wren;")
            cursor.execute(
                """
                CREATE OR REPLACE TABLE wren.t1 (
                    id INT PRIMARY KEY COMMENT 'This is a primary key',
                    value STRING
                )
                COMMENT 'This is a table comment'
                ;
                """
            )
            cursor.execute(
                """
                CREATE OR REPLACE TABLE wren.t2 (
                    id INT,
                    t1_id INT,
                    value STRING,
                    CONSTRAINT fk_t1 FOREIGN KEY (t1_id) REFERENCES wren.t1(id)
                );
                """
            )
    finally:
        connection.close()


@pytest.fixture(scope="module")
def connection_info() -> dict[str, str]:
    return {
        "databricks_type": "token",
        "serverHostname": os.getenv("TEST_DATABRICKS_SERVER_HOSTNAME"),
        "httpPath": os.getenv("TEST_DATABRICKS_HTTP_PATH"),
        "accessToken": os.getenv("TEST_DATABRICKS_TOKEN"),
    }


@pytest.fixture(scope="module")
def service_principal_connection_info() -> dict[str, str]:
    return {
        "databricks_type": "service_principal",
        "serverHostname": os.getenv("TEST_DATABRICKS_SERVER_HOSTNAME"),
        "httpPath": os.getenv("TEST_DATABRICKS_HTTP_PATH"),
        "clientId": os.getenv("TEST_DATABRICKS_CLIENT_ID"),
        "clientSecret": os.getenv("TEST_DATABRICKS_CLIENT_SECRET"),
    }
