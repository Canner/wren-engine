import logging
from json import loads

from fastapi import APIRouter

from app.mdl.rewriter import Rewriter
from app.model.data_source import DataSource
from app.model.dto import IbisDTO

logger = logging.getLogger()
router = APIRouter(prefix="/v2/ibis")


def to_json(df) -> dict:
    json_obj = loads(df.to_json(orient='split'))
    del json_obj['index']
    json_obj['dtypes'] = df.dtypes.apply(lambda x: x.name).to_dict()
    return json_obj


@router.post("/{data_source}/query")
def query(data_source: DataSource, dto: IbisDTO) -> dict:
    logger.debug(f'DTO: {dto}')
    rewritten_sql = Rewriter(dto.manifest).rewrite(dto.sql)
    return to_json(data_source.get_connection(dto.connection_info).sql(rewritten_sql, dialect='trino').to_pandas())
