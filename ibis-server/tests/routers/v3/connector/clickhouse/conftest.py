import pathlib

import pytest
from testcontainers.clickhouse import ClickHouseContainer

pytestmark = pytest.mark.clickhouse

base_url = "/v3/connector/clickhouse"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="session")
def clickhouse(request) -> ClickHouseContainer:
    ch = ClickHouseContainer("clickhouse/clickhouse-server:head-alpine", port=8123)
    ch.start()
    request.addfinalizer(ch.stop)
    return ch


@pytest.fixture(scope="module")
def connection_info(clickhouse: ClickHouseContainer) -> dict[str, str]:
    return {
        "host": clickhouse.get_container_host_ip(),
        "port": clickhouse.get_exposed_port(clickhouse.port),
        "user": clickhouse.username,
        "password": clickhouse.password,
        "database": clickhouse.dbname,
    }
