import base64
import os

import orjson


def is_docker() -> bool:
    """Detect if running inside a Docker container."""
    if os.path.exists("/.dockerenv"):
        return True
    return os.getenv("DOCKER_CONTAINER", "").lower() in ("1", "true", "yes")


def dict_to_base64_string(data: dict[str, any]) -> str:
    return base64.b64encode(orjson.dumps(data)).decode("utf-8")


def json_to_base64_string(data: str) -> str:
    return base64.b64encode(data.encode("utf-8")).decode("utf-8")
