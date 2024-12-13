import pathlib

import pytest

pytestmark = pytest.mark.anyio


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)
            item.fixturenames.append("anyio_backend")


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"
