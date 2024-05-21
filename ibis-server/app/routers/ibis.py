import logging
from json import loads

from app.model.data_source import DataSource
from app.model.dto import PostgresDTO, BigQueryDTO, SnowflakeDTO
from fastapi import APIRouter

logger = logging.getLogger()
router = APIRouter(prefix="/v2/ibis")


def to_json(df):
    json_obj = loads(df.to_json(orient='split'))
    del json_obj['index']
    json_obj['dtypes'] = df.dtypes.apply(lambda x: x.name).to_dict()
    return json_obj


@router.post("/{data_source}/query")
def query(data_source: DataSource, dto: PostgresDTO | BigQueryDTO | SnowflakeDTO):
    logger.debug(f'DTO: {dto}')
    return to_json(data_source.get_connection(dto).sql(dto.sql).to_pandas())
