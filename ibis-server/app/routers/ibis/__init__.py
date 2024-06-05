from fastapi import APIRouter

import app.routers.ibis.bigquery as bigquery
import app.routers.ibis.postgres as postgres
import app.routers.ibis.snowflake as snowflake

prefix = "/v2/ibis"

router = APIRouter(prefix=prefix)

router.include_router(bigquery.router)
router.include_router(postgres.router)
router.include_router(snowflake.router)
