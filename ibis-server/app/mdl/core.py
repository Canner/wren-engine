from functools import cache

import wren_core


@cache
def get_session_context(
    manifest_str: str | None, function_path: str
) -> wren_core.SessionContext:
    return wren_core.SessionContext(manifest_str, function_path)


def get_manifest_extractor(manifest_str: str) -> wren_core.ManifestExtractor:
    return wren_core.ManifestExtractor(manifest_str)


def to_json_base64(manifest) -> str:
    return wren_core.to_json_base64(manifest)
