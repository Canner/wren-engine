from typing import Annotated

from fastapi import APIRouter, Query, Response
from fastapi.responses import JSONResponse

from wren_engine.logger import log_dto
from wren_engine.model.connector import Connector, QueryBigQueryDTO, to_json
from wren_engine.model.data_source import DataSource
from wren_engine.model.metadata.dto import MetadataDTO, Table, Constraint
from wren_engine.model.metadata.factory import MetadataFactory
from wren_engine.model.validator import ValidateDTO, Validator

router = APIRouter(prefix="/bigquery", tags=["bigquery"])

data_source = DataSource.bigquery


@router.post("/query")
@log_dto
def query(
    dto: QueryBigQueryDTO, dry_run: Annotated[bool, Query(alias="dryRun")] = False
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


@router.post("/metadata/tables", response_model=list[Table])
@log_dto
def get_bigquery_table_list(dto: MetadataDTO) -> list[Table]:
    metadata = MetadataFactory(data_source, dto.connection_info)
    return metadata.get_table_list()


@router.post("/metadata/constraints", response_model=list[Constraint])
@log_dto
def get_bigquery_constraints(dto: MetadataDTO) -> list[Constraint]:
    metadata = MetadataFactory(data_source, dto.connection_info)
    return metadata.get_constraints()
