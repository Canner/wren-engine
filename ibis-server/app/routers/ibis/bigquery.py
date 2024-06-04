from typing import Annotated

from fastapi import APIRouter, Query

from app.logger import log_dto
from app.model.data_source import DataSource
from app.model.dto import BigQueryDTO
from app.model.coordinator import Coordinator

router = APIRouter(prefix='/bigquery', tags=['bigquery'])

data_source = DataSource.bigquery


@router.post("/query")
@log_dto
def query(dto: BigQueryDTO, dry_run: Annotated[bool, Query(alias="dryRun")] = False) -> dict:
    coord = Coordinator(data_source, dto.connection_info, dto.manifest_str)
    if dry_run:
        return coord.dry_run(dto.sql)
    return coord.query(dto.sql)
