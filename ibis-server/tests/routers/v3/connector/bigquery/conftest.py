import os
import pathlib

import pytest

pytestmark = pytest.mark.beta

base_url = "/v3/connector/bigquery"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="session")
def connection_info():
    return {
        "project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
        "dataset_id": "tpch_tiny",
        "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
    }
