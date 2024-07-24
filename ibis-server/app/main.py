from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from loguru import logger
from starlette.responses import PlainTextResponse

from app.config import get_config
from app.model import UnprocessableEntityError
from app.routers import v2, v3

app = FastAPI()
app.include_router(v2.router)
app.include_router(v3.router)

get_config().init_logger()


@app.get("/")
def root():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/config")
def config():
    return get_config()


@app.exception_handler(UnprocessableEntityError)
async def unprocessable_entity_error_handler(request, exc: UnprocessableEntityError):
    logger.exception(exc)
    return PlainTextResponse(str(exc), status_code=422)


@app.exception_handler(Exception)
async def exception_handler(request, exc: Exception):
    logger.exception(exc)
    return PlainTextResponse(str(exc), status_code=500)
