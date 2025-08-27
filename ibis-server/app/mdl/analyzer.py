import httpx
import orjson

from app.config import get_config
from app.model.error import ErrorCode, WrenError

wren_engine_endpoint = get_config().wren_engine_endpoint


def analyze(manifest_str: str, sql: str) -> list[dict]:
    try:
        r = httpx.request(
            method="GET",
            url=f"{wren_engine_endpoint}/v2/analysis/sql",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            content=orjson.dumps({"manifestStr": manifest_str, "sql": sql}),
        )
        return r.raise_for_status().json()
    except httpx.ConnectError as e:
        raise WrenError(
            ErrorCode.LEGACY_ENGINE_ERROR, f"Can not connect to Java Engine: {e}"
        ) from e
    except httpx.HTTPStatusError as e:
        raise WrenError(ErrorCode.GENERIC_USER_ERROR, e.response.text)


def analyze_batch(manifest_str: str, sqls: list[str]) -> list[list[dict]]:
    try:
        r = httpx.request(
            method="GET",
            url=f"{wren_engine_endpoint}/v2/analysis/sqls",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            content=orjson.dumps({"manifestStr": manifest_str, "sqls": sqls}),
        )
        return r.raise_for_status().json()
    except httpx.ConnectError as e:
        raise WrenError(
            ErrorCode.LEGACY_ENGINE_ERROR, f"Can not connect to Java Engine: {e}"
        ) from e
    except httpx.HTTPStatusError as e:
        raise WrenError(ErrorCode.GENERIC_USER_ERROR, e.response.text)
