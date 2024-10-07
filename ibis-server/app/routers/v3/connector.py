from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import JSONResponse

from app.dependencies import verify_query_dto
from app.mdl.rewriter import Rewriter
from app.model import (
    DryPlanDTO,
    QueryDTO,
    ValidateDTO,
)
from app.model.connector import Connector
from app.model.data_source import DataSource
from app.model.validator import Validator
from app.util import to_json

router = APIRouter(prefix="/connector")


@router.post("/{data_source}/query", dependencies=[Depends(verify_query_dto)])
def query(
    data_source: DataSource,
    dto: QueryDTO,
    dry_run: Annotated[bool, Query(alias="dryRun")] = False,
    limit: int | None = None,
) -> Response:
    rewritten_sql = Rewriter(
        dto.manifest_str, data_source=data_source, experiment=True
    ).rewrite(dto.sql)
    connector = Connector(data_source, dto.connection_info, dto.manifest_str)
    if dry_run:
        connector.dry_run(rewritten_sql)
        return Response(status_code=204)
    return JSONResponse(to_json(connector.query(rewritten_sql, limit=limit)))


@router.post("/dry-plan")
def dry_plan(dto: DryPlanDTO) -> str:
    return Rewriter(dto.manifest_str, experiment=True).rewrite(dto.sql)


@router.post("/{data_source}/dry-plan")
def dry_plan_for_data_source(data_source: DataSource, dto: DryPlanDTO) -> str:
    return Rewriter(dto.manifest_str, data_source=data_source, experiment=True).rewrite(
        dto.sql
    )


@router.post("/{data_source}/validate/{rule_name}")
def validate(data_source: DataSource, rule_name: str, dto: ValidateDTO) -> Response:
    validator = Validator(
        Connector(data_source, dto.connection_info, dto.manifest_str),
        Rewriter(dto.manifest_str, data_source=data_source, experiment=True),
    )
    validator.validate(rule_name, dto.parameters, dto.manifest_str)
    return Response(status_code=204)
