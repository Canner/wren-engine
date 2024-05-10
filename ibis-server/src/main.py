import os

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse

import routers

app = FastAPI()
app.include_router(routers.ibis_router)


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


if __name__ == "__main__":
    uvicorn.run("main:app", host='0.0.0.0', port=int(os.getenv('IBIS_PORT', 8000)), reload=True)
