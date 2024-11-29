from functools import cache

import wren_core


@cache
def get_session_context(
    manifest_str: str | None, function_path: str
) -> wren_core.SessionContext:
    return wren_core.SessionContext(manifest_str, function_path)


def resolve_used_table_names(manifest_str: str, sql: str) -> list[str]:
    return wren_core.resolve_used_table_names(manifest_str, sql)


def extract_manifest(manifest_str: str, datasets: list[str]) -> dict:
    return wren_core.extract_manifest(manifest_str, datasets)


def to_json_base64(manifest):
    return wren_core.to_json_base64(manifest)
