import pathlib
import time

import pytest
from testcontainers.trino import TrinoContainer

pytestmark = pytest.mark.trino

base_url = "/v3/connector/trino"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="session")
def trino(request) -> TrinoContainer:
    db = TrinoContainer().start()
    # To avoid `TrinoQueryError(type=INTERNAL_ERROR, name=GENERIC_INTERNAL_ERROR, message="nodes is empty")`
    time.sleep(10)
    request.addfinalizer(db.stop)
    return db


@pytest.fixture(scope="module")
def connection_info(trino: TrinoContainer) -> dict[str, str]:
    return {
        "host": trino.get_container_host_ip(),
        "port": trino.get_exposed_port(trino.port),
        "catalog": "tpch",
        "schema": "tiny",
        "user": "test",
    }
