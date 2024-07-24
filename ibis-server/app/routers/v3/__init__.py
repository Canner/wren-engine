from fastapi import APIRouter

from app.routers.v3 import connector

prefix = "/v3"

router = APIRouter(prefix=prefix)

router.include_router(connector.router)
