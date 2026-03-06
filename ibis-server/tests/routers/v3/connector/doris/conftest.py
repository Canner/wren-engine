import os
import pathlib

import pytest

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.doris

base_url = "/v3/connector/doris"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="module")
def connection_info() -> dict[str, str]:
    """Connection info for an external Doris instance.

    Set these environment variables before running tests:
      DORIS_HOST  (default: localhost)
      DORIS_PORT  (default: 9030)
      DORIS_USER  (default: root)
      DORIS_PASSWORD (default: empty)
      DORIS_DATABASE (default: test)
    """
    host = os.getenv("DORIS_HOST", "localhost")
    port = os.getenv("DORIS_PORT", "9030")
    user = os.getenv("DORIS_USER", "root")
    password = os.getenv("DORIS_PASSWORD", "")
    database = os.getenv("DORIS_DATABASE", "test")

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }


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
