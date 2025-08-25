from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TypedDict
from uuid import uuid4

from asgi_correlation_id import CorrelationIdMiddleware
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse, RedirectResponse
from loguru import logger

from app.config import get_config
from app.dependencies import X_CORRELATION_ID
from app.mdl.java_engine import JavaEngineConnector
from app.middleware import ProcessTimeMiddleware, RequestLogMiddleware
from app.model import ConfigModel
from app.model.error import ErrorCode, ErrorResponse, WrenError
from app.query_cache import QueryCacheManager
from app.routers import v2, v3

get_config().init_logger()


# Define the state of the application
# Use state to store the singleton instance
class State(TypedDict):
    java_engine_connector: JavaEngineConnector
    query_cache_manager: QueryCacheManager


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[State]:
    query_cache_manager = QueryCacheManager()

    async with JavaEngineConnector() as java_engine_connector:
        yield {
            "java_engine_connector": java_engine_connector,
            "query_cache_manager": query_cache_manager,
        }


app = FastAPI(lifespan=lifespan, title="Wren Engine API")
app.include_router(v2.router)
app.include_router(v3.router)
app.add_middleware(RequestLogMiddleware)
app.add_middleware(ProcessTimeMiddleware)
app.add_middleware(
    CorrelationIdMiddleware,
    header_name=X_CORRELATION_ID,
    generator=lambda: str(uuid4()),
)


@app.get("/")
def root():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/config")
def provide_config():
    return get_config()


@app.patch("/config")
def update_config(config_model: ConfigModel):
    config = get_config()
    config.update(diagnose=config_model.diagnose)
    return config


# In Starlette, the Exception is special and is not included in normal exception handlers.
@app.exception_handler(Exception)
def exception_handler(request, exc: Exception):
    return ORJSONResponse(
        status_code=500,
        content=ErrorResponse(
            error_code=ErrorCode.GENERIC_INTERNAL_ERROR.name,
            message=str(exc),
            timestamp=datetime.now().isoformat(),
            correlation_id=request.headers.get(X_CORRELATION_ID),
        ).model_dump(by_alias=True),
    )


# In Starlette, the exceptions other than the Exception are not raised when call_next in the middleware.
@app.exception_handler(WrenError)
def wren_error_handler(request, exc: WrenError):
    with logger.contextualize(correlation_id=request.headers.get(X_CORRELATION_ID)):
        logger.opt(exception=exc).error("Request failed")
    return ORJSONResponse(
        status_code=exc.get_http_status_code(),
        content=exc.get_response(
            correlation_id=request.headers.get(X_CORRELATION_ID)
        ).model_dump(by_alias=True),
    )


@app.exception_handler(NotImplementedError)
def not_implemented_error_handler(request, exc: NotImplementedError):
    return ORJSONResponse(
        status_code=501,
        content=ErrorResponse(
            error_code=ErrorCode.NOT_IMPLEMENTED.name,
            message=str(exc),
            timestamp=datetime.now().isoformat(),
            correlation_id=request.headers.get(X_CORRELATION_ID),
        ).model_dump(by_alias=True),
    )
