import asyncio

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


async def try_connect():
    wait_time = 30
    while wait_time > 0:
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
            await asyncio.sleep(1)
            wait_time -= 1


try:
    asyncio.get_running_loop().create_task(try_connect())
except RuntimeError:
    loop = asyncio.new_event_loop()
    loop.create_task(try_connect())  # noqa: RUF006
    asyncio.set_event_loop(loop)


def get_http_client():
    return client
