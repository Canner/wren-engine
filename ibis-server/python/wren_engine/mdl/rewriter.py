import httpx
import orjson

from wren_engine.config import get_config
from wren_engine.logger import log_rewritten

wren_engine_endpoint = get_config().wren_engine_endpoint


@log_rewritten
def rewrite(manifest_str: str, sql: str) -> str:
    try:
        r = httpx.request(
            method="GET",
            url=f"{wren_engine_endpoint}/v2/mdl/dry-plan",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            content=orjson.dumps({"manifestStr": manifest_str, "sql": sql}),
        )
        return r.text if r.status_code == httpx.codes.OK else r.raise_for_status()
    except httpx.ConnectError as e:
        raise ConnectionError(f"Can not connect to Wren Engine: {e}")
