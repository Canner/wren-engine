import os

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


def file_path(path: str) -> str:
    return os.path.join(os.path.dirname(__file__), path)


DATAFUSION_FUNCTION_COUNT = 270


@pytest.fixture(scope="module")
async def client() -> AsyncClient:
    async with AsyncClient(
        transport=ASGITransport(app), base_url="http://test"
    ) as client:
        yield client
