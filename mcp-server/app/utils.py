import base64
import orjson


def dict_to_base64_string(data: dict[str, any]) -> str:
    return base64.b64encode(orjson.dumps(data)).decode("utf-8")


def json_to_base64_string(data: str) -> str:
    return base64.b64encode(data.encode("utf-8")).decode("utf-8")
