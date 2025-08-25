import anyio
import httpcore
import httpx
from loguru import logger
from orjson import orjson

from app.config import get_config
from app.model.error import ErrorCode, ErrorPhase, WrenError

wren_engine_endpoint = get_config().wren_engine_endpoint


class JavaEngineConnector:
    def __init__(self, end_point: str | None = None):
        if end_point is None and wren_engine_endpoint is None:
            logger.warning(
                "WREN_ENGINE_ENDPOINT is not set. The v2 MDL endpoint and the fallback will not be available."
            )
            self.client = None
        else:
            self.client = httpx.AsyncClient(
                base_url=end_point or wren_engine_endpoint,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

    async def dry_plan(self, manifest_str: str, sql: str):
        if self.client is None:
            raise WrenError(
                ErrorCode.GENERIC_INTERNAL_ERROR,
                "WREN_ENGINE_ENDPOINT is not set. Cannot call dry_plan without a valid endpoint.",
                phase=ErrorPhase.SQL_PLANNING,
            )

        r = await self.client.request(
            method="GET",
            url="/v2/mdl/dry-plan",
            content=orjson.dumps({"manifestStr": manifest_str, "sql": sql}),
        )
        return r.raise_for_status().text.replace("\n", " ")

    async def _warmup(self, timeout=30):
        for _ in range(timeout):
            try:
                response = await self.client.get("/v2/health")
                if response.status_code == 200:
                    return
            except (
                httpx.ConnectError,
                httpx.HTTPStatusError,
                httpx.TimeoutException,
                httpcore.ReadTimeout,
            ):
                await anyio.sleep(1)

    async def close(self):
        if self.client is not None:
            await self.client.aclose()

    async def __aenter__(self):
        if self.client is not None:
            await self._warmup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
