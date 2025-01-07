from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TypedDict
from uuid import uuid4

from asgi_correlation_id import CorrelationIdMiddleware
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from loguru import logger
from starlette.responses import PlainTextResponse

from app.config import get_config
from app.mdl.java_engine import JavaEngineConnector
from app.middleware import ProcessTimeMiddleware, RequestLogMiddleware
from app.model import ConfigModel, CustomHttpError
from app.routers import v2, v3

get_config().init_logger()


# Define the state of the application
# Use state to store the singleton instance
class State(TypedDict):
    java_engine_connector: JavaEngineConnector


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[State]:
    async with JavaEngineConnector() as java_engine_connector:
        yield {"java_engine_connector": java_engine_connector}


app = FastAPI(lifespan=lifespan)
app.include_router(v2.router)
app.include_router(v3.router)
app.add_middleware(RequestLogMiddleware)
app.add_middleware(ProcessTimeMiddleware)
app.add_middleware(
    CorrelationIdMiddleware,
    header_name="X-Correlation-ID",
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
    return PlainTextResponse(str(exc), status_code=500)


# In Starlette, the exceptions other than the Exception are not raised when call_next in the middleware.
@app.exception_handler(CustomHttpError)
def custom_http_error_handler(request, exc: CustomHttpError):
    with logger.contextualize(correlation_id=request.headers.get("X-Correlation-ID")):
        logger.opt(exception=exc).error("Request failed")
    return PlainTextResponse(str(exc), status_code=exc.status_code)


@app.exception_handler(NotImplementedError)
def not_implemented_error_handler(request, exc: NotImplementedError):
    return PlainTextResponse(str(exc), status_code=501)
