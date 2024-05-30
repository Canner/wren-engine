from json import loads

from fastapi import APIRouter, Request

from app.logger import get_logger
from app.mdl.rewriter import Rewriter
from app.model.data_source import DataSource
from app.model.dto import PostgresDTO, BigQueryDTO, SnowflakeDTO

logger = get_logger(__name__)
router = APIRouter(prefix="/v2/ibis")


def to_json(df) -> dict:
    json_obj = loads(df.to_json(orient='split'))
    del json_obj['index']
    json_obj['dtypes'] = df.dtypes.apply(lambda x: x.name).to_dict()
    return json_obj


@router.post("/postgres/query")
def query_postgres(dto: PostgresDTO, request: Request) -> dict:
    logger.debug(f'{request.method} {request.url.path}, DTO: {dto}')
    rewritten_sql = Rewriter.rewrite(dto.manifest_str, dto.sql)
    logger.debug(f'Rewritten SQL: {rewritten_sql}')
    return to_json(DataSource.postgres.get_connection(dto.connection_info).sql(rewritten_sql, dialect='trino').to_pandas())


@router.post("/bigquery/query")
def query_bigquery(dto: BigQueryDTO, request: Request) -> dict:
    logger.debug(f'{request.method} {request.url.path}, DTO: {dto}')
    rewritten_sql = Rewriter.rewrite(dto.manifest_str, dto.sql)
    logger.debug(f'Rewritten SQL: {rewritten_sql}')
    return to_json(DataSource.bigquery.get_connection(dto.connection_info).sql(rewritten_sql, dialect='trino').to_pandas())


@router.post("/snowflake/query")
def query_snowflake(dto: SnowflakeDTO, request: Request) -> dict:
    logger.debug(f'{request.method} {request.url.path}, DTO: {dto}')
    rewritten_sql = Rewriter.rewrite(dto.manifest_str, dto.sql)
    logger.debug(f'Rewritten SQL: {rewritten_sql}')
    return to_json(DataSource.snowflake.get_connection(dto.connection_info).sql(rewritten_sql, dialect='trino').to_pandas())
