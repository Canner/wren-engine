"""MDL validation tool: JSON Schema + optional ibis-server dry-plan."""

from __future__ import annotations

import base64
import json
from typing import Any

import httpx
import jsonschema
from pydantic_ai import RunContext

from wren_agent.deps import AgentDeps

_MDL_SCHEMA_URL = (
    "https://raw.githubusercontent.com/Canner/WrenAI/main/wren-mdl/mdl.schema.json"
)

# Module-level cache: fetched once per process
_cached_schema: dict[str, Any] | None = None


def _get_mdl_schema() -> dict[str, Any]:
    global _cached_schema
    if _cached_schema is None:
        response = httpx.get(_MDL_SCHEMA_URL, timeout=10.0)
        response.raise_for_status()
        _cached_schema = response.json()
    return _cached_schema


def _dict_to_base64(data: dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(data).encode()).decode()


async def validate_mdl(
    ctx: RunContext[AgentDeps], mdl: dict[str, Any]
) -> str:
    """Validate an MDL manifest dict.

    Performs two checks:
    1. JSON Schema validation against the official Wren MDL schema.
    2. (Optional) Dry-plan validation via ibis-server if WREN_ENGINE_ENDPOINT is set.

    Returns a string describing the validation result.
    """
    # --- Step 1: JSON Schema validation ---
    try:
        schema = _get_mdl_schema()
    except Exception as e:
        return f"Warning: Could not fetch MDL schema for validation: {e}. Skipping schema validation."

    errors = list(
        jsonschema.Draft202012Validator(schema).iter_errors(mdl)
    )
    if errors:
        messages = "; ".join(
            f"{'.'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
            for e in errors[:5]
        )
        return f"JSON Schema validation failed ({len(errors)} error(s)): {messages}"

    # --- Step 2: ibis-server dry-plan (optional) ---
    ibis_url = ctx.deps.ibis_server_url
    if not ibis_url:
        return "JSON Schema validation passed. (Skipping dry-plan: WREN_ENGINE_ENDPOINT not set)"

    try:
        manifest_str = _dict_to_base64(mdl)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{ibis_url.rstrip('/')}/v3/connector/dry-plan",
                json={"manifest_str": manifest_str, "sql": "SELECT 1"},
                headers={"x-wren-fallback_disable": "true"},
            )
        if resp.status_code == 200:
            return "MDL validation passed (JSON Schema + dry-plan)."
        else:
            return (
                f"JSON Schema validation passed, but dry-plan failed "
                f"(HTTP {resp.status_code}): {resp.text[:300]}"
            )
    except Exception as e:
        return (
            f"JSON Schema validation passed, but dry-plan request failed: {e}"
        )
