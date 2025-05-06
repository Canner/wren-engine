from uvicorn.workers import UvicornWorker


class WrenUvicornWorker(UvicornWorker):
    CONFIG_KWARGS = {"loop": "uvloop", "http": "httptools"}
