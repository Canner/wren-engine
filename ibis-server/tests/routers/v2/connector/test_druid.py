import json
import time

import pytest
import requests
from testcontainers.compose import DockerCompose

pytestmark = pytest.mark.druid

base_url = "/v2/connector/druid"


def wait_for_druid_service(url, timeout=300, interval=5):
    """Wait for the Druid service to be ready.

    :param url: The URL to check.
    :param timeout: The maximum time to wait (in seconds).
    :param interval: The interval between checks (in seconds).
    :return: True if the service is ready, False if the timeout is reached.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(interval)
    return False


@pytest.fixture(scope="module")
def druid(request) -> DockerCompose:
    with DockerCompose(
        "tests/resource/druid", compose_file_name="docker-compose.yml", wait=True
    ) as compose:
        druid_url = "http://localhost:8081/status"
        if not wait_for_druid_service(druid_url):
            compose.stop()
            raise Exception("Druid service did not become ready in time")

        yield compose


def test_create_datasource(druid: DockerCompose):
    url = "http://localhost:8081/druid/indexer/v1/task"
    payload = json.dumps(
        {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "orders",
                    "timestampSpec": {"column": "timestamp_column", "format": "auto"},
                    "dimensionsSpec": {
                        "dimensions": ["dimension1", "dimension2", "dimension3"]
                    },
                    "metricsSpec": [
                        {"type": "count", "name": "count"},
                        {
                            "type": "doubleSum",
                            "name": "metric1",
                            "fieldName": "metric1",
                        },
                    ],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "day",
                        "queryGranularity": "none",
                    },
                },
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": {
                        "type": "local",
                        "baseDir": "tests/resource/tpch/data",
                        "filter": "orders.parquet",
                    },
                    "inputFormat": {"type": "parquet"},
                },
                "tuningConfig": {
                    "type": "index_parallel",
                    "maxRowsPerSegment": 5000000,
                    "maxRowsInMemory": 25000,
                },
            },
        }
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)
    assert response.status_code == 200
