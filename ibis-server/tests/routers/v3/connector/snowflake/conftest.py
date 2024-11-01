import os
import pathlib

import pytest

pytestmark = pytest.mark.snowflake

base_url = "/v3/connector/snowflake"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="module")
def connection_info() -> dict[str, str]:
    return {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "database": "SNOWFLAKE_SAMPLE_DATA",
        "schema": "TPCH_SF1",
    }
