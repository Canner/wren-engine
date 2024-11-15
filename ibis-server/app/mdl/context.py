from functools import cache


@cache
def get_session_context(manifest_str: str, function_path: str):
    from wren_core import SessionContext

    return SessionContext(manifest_str, function_path)
