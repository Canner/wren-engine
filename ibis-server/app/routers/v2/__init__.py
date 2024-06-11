from fastapi import APIRouter

from app.routers.v2 import ibis, analysis

prefix = "/v2"

router = APIRouter(prefix=prefix)

router.include_router(ibis.router)
router.include_router(analysis.router)
