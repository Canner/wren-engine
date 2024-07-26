import uuid

import orjson
from fastapi import FastAPI, Request
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


@app.middleware("http")
async def request_logger(request: Request, call_next):
    with logger.contextualize(request_id=str(uuid.uuid4())):
        logger.info("{method} {path}", method=request.method, path=request.url.path)
        logger.info("Request params: {params}", params=dict(request.query_params))
        body = await request.body()
        if body:
            json_obj = orjson.loads(body)
            if "connectionInfo" in json_obj:
                json_obj["connectionInfo"] = "REMOVED_SENSITIVE_DATA"
            body = orjson.dumps(json_obj)
        logger.info("Request body: {body}", body=body.decode("utf-8"))
        try:
            return await call_next(request)
        finally:
            logger.info("Request ended")


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
def update_config(diagnose: bool):
    config = get_config()
    config.update(diagnose=diagnose)
    return config


@app.exception_handler(UnprocessableEntityError)
def unprocessable_entity_error_handler(request, exc: UnprocessableEntityError):
    logger.exception(exc)
    return PlainTextResponse(str(exc), status_code=422)


@app.exception_handler(Exception)
def exception_handler(request, exc: Exception):
    logger.exception(exc)
    return PlainTextResponse(str(exc), status_code=500)
