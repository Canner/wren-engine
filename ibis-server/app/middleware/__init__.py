import time

from loguru import logger
from orjson import orjson
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class RequestLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID")
        with logger.contextualize(correlation_id=correlation_id):
            logger.info("{method} {path}", method=request.method, path=request.url.path)
            logger.info("Request params: {params}", params=dict(request.query_params))
            # Redact sensitive headers before logging
            sensitive = {
                "authorization",
                "proxy-authorization",
                "cookie",
                "set-cookie",
            }
            redacted_headers = {}
            for k, v in request.headers.items():
                kl = k.lower()
                if kl in sensitive:
                    redacted_headers[k] = "REDACTED"
                else:
                    redacted_headers[k] = v
            logger.info("Request headers: {headers}", headers=redacted_headers)
            body = await request.body()
            if body:
                json_obj = orjson.loads(body)
                if "connectionInfo" in json_obj:
                    json_obj["connectionInfo"] = "REDACTED"
                body = orjson.dumps(json_obj)
            logger.info("Request body: {body}", body=body.decode("utf-8"))
            try:
                return await call_next(request)
            except Exception as exc:
                logger.opt(exception=exc).error("Request failed")
                raise exc
            finally:
                logger.info("Request ended")


class ProcessTimeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.perf_counter()
        response = await call_next(request)
        process_time = time.perf_counter() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response
