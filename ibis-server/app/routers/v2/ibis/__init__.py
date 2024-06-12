from fastapi import APIRouter

import app.routers.v2.ibis.bigquery as bigquery
import app.routers.v2.ibis.postgres as postgres
import app.routers.v2.ibis.snowflake as snowflake

prefix = "/ibis"

router = APIRouter(prefix=prefix)

router.include_router(bigquery.router)
router.include_router(postgres.router)
router.include_router(snowflake.router)
