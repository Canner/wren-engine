from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import JSONResponse

from app.dependencies import verify_query_dto
from app.logger import log_dto
from app.mdl.rewriter import Rewriter
from app.model import (
    DryPlanDTO,
    QueryDTO,
    ValidateDTO,
)
from app.model.connector import Connector
from app.model.data_source import DataSource
from app.model.metadata.dto import Constraint, MetadataDTO, Table
from app.model.metadata.factory import MetadataFactory
from app.model.validator import Validator
from app.util import to_json

router = APIRouter(prefix="/connector")


@router.post("/{data_source}/query", dependencies=[Depends(verify_query_dto)])
@log_dto
def query(
    data_source: DataSource,
    dto: QueryDTO,
    dry_run: Annotated[bool, Query(alias="dryRun")] = False,
    limit: int | None = None,
) -> Response:
    connector = Connector(data_source, dto.connection_info, dto.manifest_str)
    if dry_run:
        connector.dry_run(dto.sql)
        return Response(status_code=204)
    return JSONResponse(
        to_json(connector.query(dto.sql, limit=limit), dto.column_dtypes)
    )


@router.post("/{data_source}/validate/{rule_name}")
@log_dto
def validate(data_source: DataSource, rule_name: str, dto: ValidateDTO) -> Response:
    validator = Validator(Connector(data_source, dto.connection_info, dto.manifest_str))
    validator.validate(rule_name, dto.parameters)
    return Response(status_code=204)


@router.post("/{data_source}/metadata/tables", response_model=list[Table])
@log_dto
def get_table_list(data_source: DataSource, dto: MetadataDTO) -> list[Table]:
    metadata = MetadataFactory(data_source, dto.connection_info)
    return metadata.get_table_list()


@router.post("/{data_source}/metadata/constraints", response_model=list[Constraint])
@log_dto
def get_constraints(data_source: DataSource, dto: MetadataDTO) -> list[Constraint]:
    metadata = MetadataFactory(data_source, dto.connection_info)
    return metadata.get_constraints()


@router.post("/dry-plan")
@log_dto
def dry_plan(dto: DryPlanDTO) -> str:
    return Rewriter(dto.manifest_str).rewrite(dto.sql)


@router.post("/{data_source}/dry-plan")
@log_dto
def dry_plan_for_data_source(data_source: DataSource, dto: DryPlanDTO) -> str:
    return Rewriter(dto.manifest_str, data_source).rewrite(dto.sql)
