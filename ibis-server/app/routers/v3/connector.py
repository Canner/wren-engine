from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Response
from fastapi.responses import ORJSONResponse
from loguru import logger
from opentelemetry import trace

from app.config import get_config
from app.dependencies import verify_query_dto
from app.mdl.core import get_session_context
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
from app.model.validator import Validator
from app.query_cache import QueryCacheManager
from app.routers import v2
from app.routers.v2.connector import get_java_engine_connector, get_query_cache_manager
from app.util import build_context, pushdown_limit, to_json

router = APIRouter(prefix="/connector")
tracer = trace.get_tracer(__name__)

MIGRATION_MESSAGE = "Wren engine is migrating to Rust version now. \
    Wren AI team are appreciate if you can provide the error messages and related logs for us."


@router.post("/{data_source}/query", dependencies=[Depends(verify_query_dto)])
async def query(
    data_source: DataSource,
    dto: QueryDTO,
    dry_run: Annotated[bool, Query(alias="dryRun")] = False,
    cache_enable: Annotated[bool, Query(alias="cacheEnable")] = False,
    limit: int | None = None,
    headers: Annotated[str | None, Header()] = None,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    query_cache_manager: QueryCacheManager = Depends(get_query_cache_manager),
) -> Response:
    span_name = f"v3_query_{data_source}"
    if dry_run:
        span_name += "_dry_run"
    if cache_enable:
        span_name += "_cache_enable"

    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        try:
            sql = pushdown_limit(dto.sql, limit)
            rewritten_sql = await Rewriter(
                dto.manifest_str, data_source=data_source, experiment=True
            ).rewrite(sql)
            connector = Connector(data_source, dto.connection_info)
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
        except Exception as e:
            logger.warning(
                "Failed to execute v3 query, fallback to v2: {}\n" + MIGRATION_MESSAGE,
                str(e),
            )
            return await v2.connector.query(
                data_source,
                dto,
                dry_run,
                cache_enable,
                limit,
                java_engine_connector,
                query_cache_manager,
                headers,
            )


@router.post("/dry-plan")
async def dry_plan(
    dto: DryPlanDTO,
    headers: Annotated[str | None, Header()] = None,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> str:
    with tracer.start_as_current_span(
        name="dry_plan", kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        try:
            return await Rewriter(dto.manifest_str, experiment=True).rewrite(dto.sql)
        except Exception as e:
            logger.warning(
                "Failed to execute v3 dry-plan, fallback to v2: {}\n"
                + MIGRATION_MESSAGE,
                str(e),
            )
            return await v2.connector.dry_plan(dto, java_engine_connector, headers)


@router.post("/{data_source}/dry-plan")
async def dry_plan_for_data_source(
    data_source: DataSource,
    dto: DryPlanDTO,
    headers: Annotated[str | None, Header()] = None,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> str:
    span_name = f"v3_dry_plan_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        try:
            return await Rewriter(
                dto.manifest_str, data_source=data_source, experiment=True
            ).rewrite(dto.sql)
        except Exception as e:
            logger.warning(
                "Failed to execute v3 dry-plan, fallback to v2: {}\n"
                + MIGRATION_MESSAGE,
                str(e),
            )
            return await v2.connector.dry_plan_for_data_source(
                data_source, dto, java_engine_connector, headers
            )


@router.post("/{data_source}/validate/{rule_name}")
async def validate(
    data_source: DataSource,
    rule_name: str,
    dto: ValidateDTO,
    headers: Annotated[str | None, Header()] = None,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> Response:
    span_name = f"v3_validate_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        try:
            validator = Validator(
                Connector(data_source, dto.connection_info),
                Rewriter(dto.manifest_str, data_source=data_source, experiment=True),
            )
            await validator.validate(rule_name, dto.parameters, dto.manifest_str)
            return Response(status_code=204)
        except Exception as e:
            logger.warning(
                "Failed to execute v3 validate, fallback to v2: {}\n"
                + MIGRATION_MESSAGE,
                str(e),
            )
            return await v2.connector.validate(
                data_source, rule_name, dto, java_engine_connector, headers
            )


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
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> str:
    span_name = f"v3_model-substitute_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        try:
            sql = ModelSubstitute(data_source, dto.manifest_str).substitute(dto.sql)
            Connector(data_source, dto.connection_info).dry_run(
                await Rewriter(
                    dto.manifest_str,
                    data_source=data_source,
                    experiment=True,
                ).rewrite(sql)
            )
            return sql
        except Exception as e:
            logger.warning(
                "Failed to execute v3 model-substitute, fallback to v2: {}", str(e)
            )
            return await v2.connector.model_substitute(
                data_source, dto, java_engine_connector, headers
            )
