from fastapi import APIRouter

from app.logger import log_dto
from app.mdl.analyzer import analyze, analyze_batch
from app.model import AnalyzeSQLBatchDTO, AnalyzeSQLDTO

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.get("/sql")
@log_dto
def analyze_sql(dto: AnalyzeSQLDTO) -> list[dict]:
    return analyze(dto.manifest_str, dto.sql)


@router.get("/sqls")
@log_dto
def analyze_sql_batch(dto: AnalyzeSQLBatchDTO) -> list[list[dict]]:
    return analyze_batch(dto.manifest_str, dto.sqls)
