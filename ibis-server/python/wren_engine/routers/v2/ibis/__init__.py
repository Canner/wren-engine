from fastapi import APIRouter

from wren_engine.routers.v2.ibis import bigquery, mysql, postgres, snowflake

prefix = "/ibis"

router = APIRouter(prefix=prefix)

router.include_router(bigquery.router)
router.include_router(mysql.router)
router.include_router(postgres.router)
router.include_router(snowflake.router)
