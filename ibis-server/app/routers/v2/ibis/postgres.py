from typing import Annotated

from fastapi import APIRouter, Query, Response
from fastapi.responses import JSONResponse

from app.logger import log_dto
from app.model.connector import Connector, QueryPostgresDTO, to_json
from app.model.data_source import DataSource
from app.model.validator import ValidateDTO, Validator
from app.model.metadata.dto import MetadataDTO, Table, Constraint
from app.model.metadata.factory import MetadataFactory

router = APIRouter(prefix="/postgres", tags=["postgres"])

data_source = DataSource.postgres


@router.post("/query")
@log_dto
def query(
    dto: QueryPostgresDTO, dry_run: Annotated[bool, Query(alias="dryRun")] = False
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
def get_postgres_table_list(dto: MetadataDTO) -> list[Table]:
    metadata = MetadataFactory(DataSource.postgres, dto.connection_info)
    return metadata.get_table_list()


@router.post("/metadata/constraints", response_model=list[Constraint])
@log_dto
def get_postgres_constraints(dto: MetadataDTO) -> list[Constraint]:
    metadata = MetadataFactory(DataSource.postgres, dto.connection_info)
    return metadata.get_constraints()
