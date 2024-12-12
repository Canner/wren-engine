import anyio
import httpcore
import httpx

from app.config import get_config

wren_engine_endpoint = get_config().wren_engine_endpoint


client = httpx.AsyncClient(
    base_url=wren_engine_endpoint,
    headers={
        "Content-Type": "application/json",
        "Accept": "application/json",
    },
)


async def warmup_http_client(timeout=30):
    for _ in range(timeout):
        try:
            response = await client.get("/v2/health")
            if response.status_code == 200:
                return
        except (
            httpx.ConnectError,
            httpx.HTTPStatusError,
            httpx.TimeoutException,
            httpcore.ReadTimeout,
        ):
            await anyio.sleep(1)


def get_http_client() -> httpx.AsyncClient:
    return client
