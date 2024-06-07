from typing import Annotated

from fastapi import APIRouter, Query, Response
from fastapi.responses import JSONResponse

from app.logger import log_dto
from app.model.connector import Connector, QueryBigQueryDTO, to_json
from app.model.data_source import DataSource
from app.model.validator import ValidateDTO, Validator
from app.model.dto import BigQueryDTO
from app.model.metadata_dto import MetadataDTO
from app.model.metadata import Metadata

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


@router.post("/metadata/tables")
@log_dto
def get_bigquery_table_list(dto: MetadataDTO) -> dict:
    table_list = Metadata.bigquery.get_table_list(dto.connection_info)
    return {"tables": table_list}


@router.post("/metadata/constraints")
@log_dto
def get_bigquery_constraints(dto: MetadataDTO) -> dict:
    table_list = Metadata.bigquery.get_constraints(dto.connection_info)
    return {"constraints": table_list}
