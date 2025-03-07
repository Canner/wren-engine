from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Response
from fastapi.responses import ORJSONResponse
from opentelemetry import trace

from app.config import get_config
from app.dependencies import verify_query_dto
from app.mdl.core import get_session_context
from app.mdl.rewriter import Rewriter
from app.mdl.substitute import ModelSubstitute
from app.model import (
    DryPlanDTO,
    QueryDTO,
    TranspileDTO,
    ValidateDTO,
)
from app.model.connector import Connector
from app.model.data_source import DataSource
from app.model.validator import Validator
from app.util import build_context, pushdown_limit, to_json

router = APIRouter(prefix="/connector")
tracer = trace.get_tracer(__name__)


@router.post("/{data_source}/query", dependencies=[Depends(verify_query_dto)])
async def query(
    data_source: DataSource,
    dto: QueryDTO,
    dry_run: Annotated[bool, Query(alias="dryRun")] = False,
    limit: int | None = None,
    headers: Annotated[str | None, Header()] = None,
) -> Response:
    span_name = (
        f"v3_query_{data_source}_dry_run" if dry_run else f"v3_query_{data_source}"
    )
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        sql = pushdown_limit(dto.sql, limit)
        rewritten_sql = await Rewriter(
            dto.manifest_str, data_source=data_source, experiment=True
        ).rewrite(sql)
        connector = Connector(data_source, dto.connection_info)
        if dry_run:
            connector.dry_run(rewritten_sql)
            return Response(status_code=204)
        return ORJSONResponse(to_json(connector.query(rewritten_sql, limit=limit)))


@router.post("/dry-plan")
async def dry_plan(
    dto: DryPlanDTO,
    headers: Annotated[str | None, Header()] = None,
) -> str:
    with tracer.start_as_current_span(
        name="dry_plan", kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        return await Rewriter(dto.manifest_str, experiment=True).rewrite(dto.sql)


@router.post("/{data_source}/dry-plan")
async def dry_plan_for_data_source(
    data_source: DataSource,
    dto: DryPlanDTO,
    headers: Annotated[str | None, Header()] = None,
) -> str:
    span_name = f"v3_dry_plan_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        return await Rewriter(
            dto.manifest_str, data_source=data_source, experiment=True
        ).rewrite(dto.sql)


@router.post("/{data_source}/validate/{rule_name}")
async def validate(
    data_source: DataSource,
    rule_name: str,
    dto: ValidateDTO,
    headers: Annotated[str | None, Header()] = None,
) -> Response:
    span_name = f"v3_validate_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        validator = Validator(
            Connector(data_source, dto.connection_info),
            Rewriter(dto.manifest_str, data_source=data_source, experiment=True),
        )
        await validator.validate(rule_name, dto.parameters, dto.manifest_str)
        return Response(status_code=204)


@router.get("/{data_source}/functions")
def functions(
    data_source: DataSource,
    headers: Annotated[str | None, Header()] = None,
) -> Response:
    span_name = f"v3_functions_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        file_path = get_config().get_remote_function_list_path(data_source)
        session_context = get_session_context(None, file_path)
        func_list = [f.to_dict() for f in session_context.get_available_functions()]
        return ORJSONResponse(func_list)


@router.post("/{data_source}/model-substitute")
async def model_substitute(
    data_source: DataSource,
    dto: TranspileDTO,
    headers: Annotated[str | None, Header()] = None,
) -> str:
    span_name = f"v3_model-substitute_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        sql = ModelSubstitute(data_source, dto.manifest_str).substitute(dto.sql)
        Connector(data_source, dto.connection_info).dry_run(
            await Rewriter(
                dto.manifest_str,
                data_source=data_source,
                experiment=True,
            ).rewrite(sql)
        )
        return sql
