from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from fastapi.responses import ORJSONResponse
from loguru import logger
from opentelemetry import trace

from app.dependencies import verify_query_dto
from app.mdl.java_engine import JavaEngineConnector
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
from app.model.metadata.dto import Constraint, MetadataDTO, Table
from app.model.metadata.factory import MetadataFactory
from app.model.validator import Validator
from app.query_cache import QueryCacheManager
from app.util import build_context, pushdown_limit, to_json

router = APIRouter(prefix="/connector")
tracer = trace.get_tracer(__name__)


def get_java_engine_connector(request: Request) -> JavaEngineConnector:
    return request.state.java_engine_connector


def get_query_cache_manager(request: Request) -> QueryCacheManager:
    return request.state.query_cache_manager


@router.post(
    "/{data_source}/query", dependencies=[Depends(verify_query_dto)], deprecated=True
)
async def query(
    data_source: DataSource,
    dto: QueryDTO,
    dry_run: Annotated[bool, Query(alias="dryRun")] = False,
    cache_enable: Annotated[bool, Query(alias="cacheEnable")] = False,
    limit: int | None = None,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    query_cache_manager: QueryCacheManager = Depends(get_query_cache_manager),
    headers: Annotated[str | None, Header()] = None,
) -> Response:
    span_name = f"v2_query_{data_source}"
    if dry_run:
        span_name += "_dry_run"
    if cache_enable:
        span_name += "_cache_enable"

    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        try:
            sql = pushdown_limit(dto.sql, limit)
        except Exception as e:
            logger.warning("Failed to pushdown limit. Using original SQL: {}", e)
            sql = dto.sql

        rewritten_sql = await Rewriter(
            dto.manifest_str,
            data_source=data_source,
            java_engine_connector=java_engine_connector,
        ).rewrite(sql)
        connector = Connector(data_source, dto.connection_info)

        # First check if the query is a dry run
        # If it is dry run.
        # We don't need to check query cache
        if dry_run:
            connector.dry_run(rewritten_sql)
            return Response(status_code=204)

        # Not a dry run
        # Check if the query is cached
        cached_result = None
        cache_hit = False

        if cache_enable:
            cached_result = query_cache_manager.get(
                str(data_source), dto.sql, dto.connection_info
            )
            cache_hit = cached_result is not None

        if cache_hit:
            span.add_event("cache hit")
            response = ORJSONResponse(to_json(cached_result))
            response.headers["X-Cache-Hit"] = str(cache_hit).lower()
            return response
        else:
            result = connector.query(rewritten_sql, limit=limit)
            if cache_enable:
                query_cache_manager.set(
                    data_source, dto.sql, result, dto.connection_info
                )

            response = ORJSONResponse(to_json(result))
            response.headers["X-Cache-Hit"] = str(cache_hit).lower()
            return response


@router.post("/{data_source}/validate/{rule_name}", deprecated=True)
async def validate(
    data_source: DataSource,
    rule_name: str,
    dto: ValidateDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    headers: Annotated[str | None, Header()] = None,
) -> Response:
    span_name = f"v2_validate_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        validator = Validator(
            Connector(data_source, dto.connection_info),
            Rewriter(
                dto.manifest_str,
                data_source=data_source,
                java_engine_connector=java_engine_connector,
            ),
        )
        await validator.validate(rule_name, dto.parameters, dto.manifest_str)
        return Response(status_code=204)


@router.post(
    "/{data_source}/metadata/tables", response_model=list[Table], deprecated=True
)
def get_table_list(
    data_source: DataSource,
    dto: MetadataDTO,
    headers: Annotated[str | None, Header()] = None,
) -> list[Table]:
    span_name = f"v2_metadata_tables_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        return MetadataFactory.get_metadata(
            data_source, dto.connection_info
        ).get_table_list()


@router.post(
    "/{data_source}/metadata/constraints",
    response_model=list[Constraint],
    deprecated=True,
)
def get_constraints(
    data_source: DataSource,
    dto: MetadataDTO,
    headers: Annotated[str | None, Header()] = None,
) -> list[Constraint]:
    span_name = f"v2_metadata_constraints_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        return MetadataFactory.get_metadata(
            data_source, dto.connection_info
        ).get_constraints()


@router.post("/{data_source}/metadata/version", deprecated=True)
def get_db_version(data_source: DataSource, dto: MetadataDTO) -> str:
    return MetadataFactory.get_metadata(data_source, dto.connection_info).get_version()


@router.post("/dry-plan", deprecated=True)
async def dry_plan(
    dto: DryPlanDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    headers: Annotated[str | None, Header()] = None,
) -> str:
    with tracer.start_as_current_span(
        name="dry_plan", kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        return await Rewriter(
            dto.manifest_str, java_engine_connector=java_engine_connector
        ).rewrite(dto.sql)


@router.post("/{data_source}/dry-plan", deprecated=True)
async def dry_plan_for_data_source(
    data_source: DataSource,
    dto: DryPlanDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    headers: Annotated[str | None, Header()] = None,
) -> str:
    span_name = f"v2_dry_plan_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        return await Rewriter(
            dto.manifest_str,
            data_source=data_source,
            java_engine_connector=java_engine_connector,
        ).rewrite(dto.sql)


@router.post("/{data_source}/model-substitute", deprecated=True)
async def model_substitute(
    data_source: DataSource,
    dto: TranspileDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    headers: Annotated[str | None, Header()] = None,
) -> str:
    span_name = f"v2_model_substitute_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        sql = ModelSubstitute(data_source, dto.manifest_str).substitute(
            dto.sql, write="trino"
        )
        Connector(data_source, dto.connection_info).dry_run(
            await Rewriter(
                dto.manifest_str,
                data_source=data_source,
                java_engine_connector=java_engine_connector,
            ).rewrite(sql)
        )
        return sql
