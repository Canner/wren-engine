from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import ORJSONResponse
from loguru import logger
from opentelemetry import trace
from starlette.datastructures import Headers

from app.dependencies import (
    X_CACHE_CREATE_AT,
    X_CACHE_HIT,
    X_CACHE_OVERRIDE,
    X_CACHE_OVERRIDE_AT,
    get_wren_headers,
    verify_query_dto,
)
from app.mdl.java_engine import JavaEngineConnector
from app.mdl.rewriter import Rewriter
from app.mdl.substitute import ModelSubstitute
from app.model import (
    BigQueryProjectConnectionInfo,
    DryPlanDTO,
    QueryDTO,
    TranspileDTO,
    ValidateDTO,
)
from app.model.connector import Connector
from app.model.data_source import DataSource
from app.model.error import ErrorCode, WrenError
from app.model.metadata.dto import (
    Constraint,
    Table,
    V2MetadataDTO,
)
from app.model.metadata.factory import MetadataFactory
from app.model.validator import Validator
from app.query_cache import QueryCacheManager
from app.util import (
    build_context,
    execute_dry_run_with_timeout,
    execute_get_constraints_with_timeout,
    execute_get_table_list_with_timeout,
    execute_get_version_with_timeout,
    execute_query_with_timeout,
    execute_validate_with_timeout,
    get_fallback_message,
    pushdown_limit,
    set_attribute,
    to_json,
    update_response_headers,
)

router = APIRouter(prefix="/connector", tags=["connector"])
tracer = trace.get_tracer(__name__)


def get_java_engine_connector(request: Request) -> JavaEngineConnector:
    return request.state.java_engine_connector


def get_query_cache_manager(request: Request) -> QueryCacheManager:
    return request.state.query_cache_manager


@router.post(
    "/{data_source}/query",
    dependencies=[Depends(verify_query_dto)],
    deprecated=True,
    description="query the specified data source",
)
async def query(
    headers: Annotated[Headers, Depends(get_wren_headers)],
    data_source: DataSource,
    dto: QueryDTO,
    dry_run: Annotated[
        bool,
        Query(alias="dryRun", description="enable dryRun mode for validating SQL only"),
    ] = False,
    cache_enable: Annotated[
        bool, Query(alias="cacheEnable", description="enable query cache mode")
    ] = False,
    override_cache: Annotated[
        bool, Query(alias="overrideCache", description="ovrride the exist cache")
    ] = False,
    limit: int | None = Query(None, description="limit the number of rows returned"),
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    query_cache_manager: QueryCacheManager = Depends(get_query_cache_manager),
    is_fallback: bool | None = None,
) -> Response:
    span_name = f"v2_query_{data_source}"
    if dry_run:
        span_name += "_dry_run"
    if cache_enable:
        span_name += "_cache_enable"
    connection_info = data_source.get_connection_info(
        dto.connection_info, dict(headers)
    )
    # Convert headers to dict for cache manager
    headers_dict = dict(headers) if headers else None

    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        try:
            sql = pushdown_limit(dto.sql, limit)
        except Exception as e:
            logger.warning("Failed to pushdown limit. Using original SQL: {}", e)
            sql = dto.sql

        # First check if the query is a dry run
        # If it is dry run.
        # We don't need to check query cache
        if dry_run:
            rewritten_sql = await Rewriter(
                dto.manifest_str,
                data_source=data_source,
                java_engine_connector=java_engine_connector,
            ).rewrite(sql)
            connector = Connector(data_source, connection_info)
            await execute_dry_run_with_timeout(
                connector,
                rewritten_sql,
            )
            return Response(status_code=204)

        # Not a dry run
        # Check if the query is cached
        cached_result = None
        cache_hit = False

        if cache_enable:
            cached_result = query_cache_manager.get(
                data_source, dto.sql, connection_info, headers_dict
            )
            cache_hit = cached_result is not None

        cache_headers = {}
        # case 1: cache hit read
        if cache_enable and cache_hit and not override_cache:
            span.add_event("cache hit")
            result = cached_result
            cache_headers[X_CACHE_HIT] = "true"
            cache_headers[X_CACHE_CREATE_AT] = str(
                query_cache_manager.get_cache_file_timestamp(
                    data_source, dto.sql, connection_info, headers_dict
                )
            )
        # all other cases require rewriting + connecting
        else:
            rewritten_sql = await Rewriter(
                dto.manifest_str,
                data_source=data_source,
                java_engine_connector=java_engine_connector,
            ).rewrite(sql)
            connector = Connector(data_source, connection_info)
            result = await execute_query_with_timeout(
                connector,
                rewritten_sql,
            )

            # headers for all non-hit cases
            cache_headers[X_CACHE_HIT] = "false"

            # case 2 cache hit but override cache
            if cache_enable and cache_hit and override_cache:
                cache_headers[X_CACHE_CREATE_AT] = str(
                    query_cache_manager.get_cache_file_timestamp(
                        data_source,
                        dto.sql,
                        connection_info,
                        headers_dict,
                    )
                )
                query_cache_manager.set(
                    data_source,
                    dto.sql,
                    result,
                    connection_info,
                    headers_dict,
                )

                cache_headers[X_CACHE_OVERRIDE] = "true"
                cache_headers[X_CACHE_OVERRIDE_AT] = str(
                    query_cache_manager.get_cache_file_timestamp(
                        data_source,
                        dto.sql,
                        connection_info,
                        headers_dict,
                    )
                )
            # case 3/4: cache miss but enabled (need to create cache)
            # no matter the cache override or not, we need to create cache
            elif cache_enable and not cache_hit:
                query_cache_manager.set(
                    data_source,
                    dto.sql,
                    result,
                    connection_info,
                    headers_dict,
                )
            # case 5~8 Other cases (cache is not enabled)
            elif not cache_enable:
                pass
        response = ORJSONResponse(to_json(result, headers, data_source=data_source))
        update_response_headers(response, cache_headers)

        if is_fallback:
            get_fallback_message(
                logger, "query", data_source, dto.manifest_str, dto.sql
            )

        return response


@router.post(
    "/{data_source}/validate/{rule_name}",
    deprecated=True,
    description="validate the specified rule",
)
async def validate(
    headers: Annotated[Headers, Depends(get_wren_headers)],
    data_source: DataSource,
    rule_name: str,
    dto: ValidateDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    is_fallback: bool | None = None,
) -> Response:
    span_name = f"v2_validate_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        validator = Validator(
            Connector(data_source, connection_info),
            Rewriter(
                dto.manifest_str,
                data_source=data_source,
                java_engine_connector=java_engine_connector,
            ),
        )
        await execute_validate_with_timeout(
            validator,
            rule_name,
            dto.parameters,
            dto.manifest_str,
        )
        response = Response(status_code=204)
        if is_fallback:
            get_fallback_message(
                logger, "validate", data_source, dto.manifest_str, None
            )
        return response


@router.post(
    "/{data_source}/metadata/tables",
    response_model=list[Table],
    deprecated=True,
    description="get the table list of the specified data source",
)
async def get_table_list(
    data_source: DataSource,
    dto: V2MetadataDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
) -> list[Table]:
    span_name = f"v2_metadata_tables_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        if isinstance(connection_info, BigQueryProjectConnectionInfo):
            raise WrenError(
                ErrorCode.INVALID_CONNECTION_INFO,
                "BigQuery project-level connection info is only supported by v3 API for metadata table list retrieval.",
            )
        else:
            metadata = MetadataFactory.get_metadata(data_source, connection_info)
            return await execute_get_table_list_with_timeout(metadata)


@router.post(
    "/{data_source}/metadata/constraints",
    response_model=list[Constraint],
    deprecated=True,
    description="get the constraints of the specified data source",
)
async def get_constraints(
    data_source: DataSource,
    dto: V2MetadataDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
) -> list[Constraint]:
    span_name = f"v2_metadata_constraints_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        if isinstance(connection_info, BigQueryProjectConnectionInfo):
            raise WrenError(
                ErrorCode.INVALID_CONNECTION_INFO,
                "BigQuery project-level connection info is only supported by v3 API for metadata constraints retrieval.",
            )
        metadata = MetadataFactory.get_metadata(data_source, connection_info)
        return await execute_get_constraints_with_timeout(metadata)


@router.post(
    "/{data_source}/metadata/version",
    deprecated=True,
    description="get the version of the specified data source",
)
async def get_db_version(
    data_source: DataSource,
    dto: V2MetadataDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
) -> str:
    connection_info = data_source.get_connection_info(
        dto.connection_info, dict(headers)
    )
    metadata = MetadataFactory.get_metadata(data_source, connection_info)
    return await execute_get_version_with_timeout(metadata)


@router.post("/dry-plan", deprecated=True, description="get the planned WrenSQL")
async def dry_plan(
    headers: Annotated[Headers, Depends(get_wren_headers)],
    dto: DryPlanDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    is_fallback: bool | None = None,
) -> str:
    with tracer.start_as_current_span(
        name="dry_plan", kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        sql = await Rewriter(
            dto.manifest_str, java_engine_connector=java_engine_connector
        ).rewrite(dto.sql)

        if is_fallback:
            get_fallback_message(logger, "dry_plan", None, dto.manifest_str, dto.sql)

        return sql


@router.post(
    "/{data_source}/dry-plan",
    deprecated=True,
    description="get the dialect SQL for the specified data source",
)
async def dry_plan_for_data_source(
    headers: Annotated[Headers, Depends(get_wren_headers)],
    data_source: DataSource,
    dto: DryPlanDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    is_fallback: bool | None = None,
) -> str:
    span_name = f"v2_dry_plan_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        sql = await Rewriter(
            dto.manifest_str,
            data_source=data_source,
            java_engine_connector=java_engine_connector,
        ).rewrite(dto.sql)
        if is_fallback:
            get_fallback_message(
                logger, "dry_plan", data_source, dto.manifest_str, dto.sql
            )
        return sql


@router.post(
    "/{data_source}/model-substitute",
    deprecated=True,
    description="get the SQL which table name is substituted",
)
async def model_substitute(
    data_source: DataSource,
    dto: TranspileDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    is_fallback: bool | None = None,
) -> str:
    span_name = f"v2_model_substitute_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        sql = ModelSubstitute(data_source, dto.manifest_str, headers).substitute(
            dto.sql, write="trino"
        )
        connector = Connector(data_source, connection_info)
        rewritten_sql = await Rewriter(
            dto.manifest_str,
            data_source=data_source,
            java_engine_connector=java_engine_connector,
        ).rewrite(sql)
        await execute_dry_run_with_timeout(
            connector,
            rewritten_sql,
        )
        if is_fallback:
            get_fallback_message(
                logger, "model_substitute", data_source, dto.manifest_str, dto.sql
            )
        return sql
