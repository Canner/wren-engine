from typing import Annotated

from fastapi import APIRouter, Query, Response
from fastapi.responses import JSONResponse

from app.logger import log_dto
from app.model.connector import Connector
from app.model.data_source import DataSource
from app.model.dto import PostgresDTO

router = APIRouter(prefix='/postgres', tags=['postgres'])

data_source = DataSource.postgres


@router.post("/query")
@log_dto
def query(dto: PostgresDTO, dry_run: Annotated[bool, Query(alias="dryRun")] = False) -> Response:
    connector = Connector(data_source, dto.connection_info, dto.manifest_str, dto.column_dtypes)
    if dry_run:
        connector.dry_run(dto.sql)
        return Response(status_code=204)
    return JSONResponse(connector.query(dto.sql))
