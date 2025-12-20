import os
import pathlib

import pytest

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.bigquery

base_url = "/v3/connector/bigquery"

function_list_path = file_path("../resources/function_list")
white_function_list_path = file_path("../resources/white_function_list")


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


@pytest.fixture(scope="session")
def project_connection_info():
    return {
        "bigquery_type": "project",
        "billing_project_id": os.getenv("TEST_BIG_QUERY_PROJECT_ID"),
        "region": os.getenv("TEST_BIG_QUERY_REGION", "asia-east1"),
        "credentials": os.getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"),
    }


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    config.set_remote_white_function_list_path(white_function_list_path)
    yield
    config.set_remote_function_list_path(None)
    config.set_remote_white_function_list_path(None)
