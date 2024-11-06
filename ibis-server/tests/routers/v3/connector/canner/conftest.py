import os
import pathlib

import pytest

"""
The Canner Enterprise must setup below:
- A user with PAT
- A data source with TPCH Tiny
- A workspace
- A TPCH table `orders` in the workspace
- The table `orders` must be with a description `This is a table comment`
- The table `orders` must have a column `o_comment` with a description `This is a comment`
"""

pytestmark = pytest.mark.canner

base_url = "/v3/connector/canner"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="module")
def connection_info() -> dict[str, str]:
    return {
        "host": os.getenv("CANNER_HOST", default="localhost"),
        "port": os.getenv("CANNER_PORT", default="7432"),
        "user": os.getenv("CANNER_USER", default="canner"),
        "pat": os.getenv("CANNER_PAT", default="PAT"),
        "workspace": os.getenv("CANNER_WORKSPACE", default="ws"),
    }
