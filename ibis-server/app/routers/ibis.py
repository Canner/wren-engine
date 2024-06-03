from fastapi import APIRouter

from app.mdl.rewriter import rewrite
from app.model.data_source import DataSource, ConnectionInfo
from app.model.dto import PostgresDTO, BigQueryDTO, SnowflakeDTO
from app.util import log_dto, to_json

router = APIRouter(prefix="/v2/ibis")


@router.post("/postgres/query")
@log_dto
def query_postgres(dto: PostgresDTO) -> dict:
    rewritten_sql = rewrite(dto.manifest_str, dto.sql)
    return query_data(DataSource.postgres, dto.connection_info, rewritten_sql)


@router.post("/bigquery/query")
@log_dto
def query_bigquery(dto: BigQueryDTO) -> dict:
    rewritten_sql = rewrite(dto.manifest_str, dto.sql)
    return query_data(DataSource.bigquery, dto.connection_info, rewritten_sql)


@router.post("/snowflake/query")
@log_dto
def query_snowflake(dto: SnowflakeDTO) -> dict:
    rewritten_sql = rewrite(dto.manifest_str, dto.sql)
    return query_data(DataSource.snowflake, dto.connection_info, rewritten_sql)


def query_data(data_source: DataSource, connection_info: ConnectionInfo, rewritten_sql: str) -> dict:
    return to_json(data_source.get_connection(connection_info).sql(rewritten_sql, dialect='trino').to_pandas())
