import httpx
import orjson

from app.config import get_config

wren_engine_endpoint = get_config().wren_engine_endpoint


def analyze(manifest_str: str, sql: str) -> list[dict]:
    try:
        r = httpx.request(
            method="GET",
            url=f"{wren_engine_endpoint}/v2/analysis/sql",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            content=orjson.dumps({"manifestStr": manifest_str, "sql": sql}),
        )
        return r.json() if r.status_code == httpx.codes.OK else r.raise_for_status()
    except httpx.ConnectError as e:
        raise ConnectionError(f"Can not connect to Wren Engine: {e}") from e
