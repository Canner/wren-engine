from fastapi import APIRouter

from app.logger import log_dto
from app.mdl.analyzer import analyze
from app.model import AnalyzeSQLDTO

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.get("/sql")
@log_dto
def analyze_sql(dto: AnalyzeSQLDTO) -> list[dict]:
    return analyze(dto.manifest_str, dto.sql)
