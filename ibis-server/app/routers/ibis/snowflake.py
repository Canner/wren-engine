from typing import Annotated

from fastapi import APIRouter, Query

from app.logger import log_dto
from app.model.data_source import DataSource
from app.model.dto import SnowflakeDTO
from app.model.coordinator import Coordinator

router = APIRouter(prefix='/snowflake', tags=['snowflake'])

data_source = DataSource.snowflake


@router.post("/query")
@log_dto
def query(dto: SnowflakeDTO, dry_run: Annotated[bool, Query(alias="dryRun")] = False) -> dict:
    coord = Coordinator(data_source, dto.connection_info, dto.manifest_str)
    if dry_run:
        return coord.dry_run(dto.sql)
    return coord.query(dto.sql)
