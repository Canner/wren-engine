from fastapi import APIRouter

from app.routers.v2 import analysis, connector

prefix = "/v2"

router = APIRouter(prefix=prefix)

router.include_router(connector.router)
router.include_router(analysis.router)
