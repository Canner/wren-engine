from fastapi import Request
from starlette.datastructures import Headers

from app.model import QueryDTO
from app.model.data_source import DataSource

X_WREN_FALLBACK_DISABLE = "x-wren-fallback_disable"
X_WREN_VARIABLE_PREFIX = "x-wren-variable-"
X_WREN_TIMEZONE = "x-wren-timezone"
X_CACHE_HIT = "X-Cache-Hit"
X_CACHE_CREATE_AT = "X-Cache-Create-At"
X_CACHE_OVERRIDE = "X-Cache-Override"
X_CACHE_OVERRIDE_AT = "X-Cache-Override-At"


# Rebuild model to validate the dto is correct via validation of the pydantic
def verify_query_dto(data_source: DataSource, dto: QueryDTO):
    data_source.get_dto_type()(**dto.model_dump(by_alias=True))


def get_wren_headers(request: Request) -> Headers:
    return Headers(
        raw=list(
            filter(
                lambda t: _filter_headers(t[0].decode("latin-1")),
                request.headers.raw,
            )
        )
    )


def _filter_headers(header_string: str) -> bool:
    if header_string.startswith("x-wren-"):
        return True
    elif header_string.startswith("x-user-"):
        return True
    elif header_string.startswith("x-correlation-id"):
        return True
    elif header_string == "traceparent":
        return True
    elif header_string == "tracestate":
        return True
    elif header_string == "sentry-trace":
        return True
    return False


def exist_wren_variables_header(
    headers: Headers,
) -> bool:
    if headers is None:
        return False
    return any(key.startswith(X_WREN_VARIABLE_PREFIX) for key in headers.keys())
