import anyio
import httpcore
import httpx
from orjson import orjson

from app.config import get_config

wren_engine_endpoint = get_config().wren_engine_endpoint


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class JavaEngineConnector(metaclass=Singleton):
    def __init__(self):
        self.client = None

    def start(self):
        self.client = httpx.AsyncClient(
            base_url=wren_engine_endpoint,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

    async def dry_plan(self, manifest_str: str, sql: str):
        r = await self.client.request(
            method="GET",
            url="/v2/mdl/dry-plan",
            content=orjson.dumps({"manifestStr": manifest_str, "sql": sql}),
        )
        return r.raise_for_status().text.replace("\n", " ")

    async def warmup(self, timeout=30):
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
        await self.client.aclose()


def get_java_engine_connector() -> JavaEngineConnector:
    return JavaEngineConnector()
