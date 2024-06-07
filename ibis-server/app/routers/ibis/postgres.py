from typing import Annotated

from fastapi import APIRouter, Query, Response
from fastapi.responses import JSONResponse

from app.logger import log_dto
from app.model.data_source import DataSource
from app.model.querier import Querier, QueryPostgresDTO
from app.model.validator import ValidateDTO, Validator

router = APIRouter(prefix="/postgres", tags=["postgres"])

data_source = DataSource.postgres


@router.post("/query")
@log_dto
def query(
    dto: QueryPostgresDTO, dry_run: Annotated[bool, Query(alias="dryRun")] = False
) -> Response:
    querier = Querier(
        data_source, dto.connection_info, dto.manifest_str, dto.column_dtypes
    )
    if dry_run:
        querier.dry_run(dto.sql)
        return Response(status_code=204)
    return JSONResponse(querier.query(dto.sql))


@router.post("/validate/{rule_name}")
@log_dto
def validate(rule_name: str, dto: ValidateDTO) -> Response:
    validator = Validator(data_source, dto.connection_info, dto.manifest_str)
    validator.validate(rule_name, dto.parameters)
    return Response(status_code=204)
