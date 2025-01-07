import pathlib

import pytest

pytestmark = pytest.mark.local_file

base_url = "/v3/connector/local_file"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        try:
            pathlib.Path(item.fspath).relative_to(current_file_dir)
            item.add_marker(pytestmark)
        except ValueError:
            pass


@pytest.fixture(scope="module")
def connection_info() -> dict[str, str]:
    return {
        "url": "tests/resource/tpch/data",
        "format": "parquet",
    }
