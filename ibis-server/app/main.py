from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse

from app.routers import ibis

app = FastAPI()
app.include_router(ibis.router)


@app.get("/")
def root():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.exception_handler(Exception)
async def exception_handler(request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content=str(exc),
    )
