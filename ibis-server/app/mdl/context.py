from functools import cache

from wren_core import SessionContext


@cache
def get_session_context(manifest_str: str, function_path: str) -> SessionContext:
    return SessionContext(manifest_str, function_path)
