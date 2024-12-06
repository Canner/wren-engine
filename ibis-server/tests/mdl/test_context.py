import base64

import orjson

from app.mdl.core import get_session_context
from tests.conftest import file_path


def test_cache():
    manifest = {
        "catalog": "my_catalog",
        "schema": "my_schema",
        "models": [
            {
                "name": "Orders",
                "refSql": "select * from public.orders",
                "columns": [
                    {"name": "orderkey", "expression": "o_orderkey", "type": "integer"}
                ],
            },
        ],
    }
    manifest_str = base64.b64encode(orjson.dumps(manifest)).decode("utf-8")
    function_path = file_path("../resources/function_list")
    session_context_1 = get_session_context(manifest_str, function_path)
    session_context_2 = get_session_context(manifest_str, function_path)
    assert session_context_1 is session_context_2
