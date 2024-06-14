from typing import Annotated

from fastapi import APIRouter, Query, Response
from fastapi.responses import JSONResponse

from app.logger import log_dto
from app.model.connector import Connector, to_json, QueryMySqlDTO
from app.model.data_source import DataSource
from app.model.validator import ValidateDTO, Validator

router = APIRouter(prefix="/mysql", tags=["mysql"])

data_source = DataSource.mysql


@router.post("/query")
@log_dto
def query(
    dto: QueryMySqlDTO, dry_run: Annotated[bool, Query(alias="dryRun")] = False
) -> Response:
    connector = Connector(data_source, dto.connection_info, dto.manifest_str)
    if dry_run:
        connector.dry_run(dto.sql)
        return Response(status_code=204)
    return JSONResponse(to_json(connector.query(dto.sql), dto.column_dtypes))


@router.post("/validate/{rule_name}")
@log_dto
def validate(rule_name: str, dto: ValidateDTO) -> Response:
    validator = Validator(Connector(data_source, dto.connection_info, dto.manifest_str))
    validator.validate(rule_name, dto.parameters)
    return Response(status_code=204)
