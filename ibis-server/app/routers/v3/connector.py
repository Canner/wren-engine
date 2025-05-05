from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import ORJSONResponse
from loguru import logger
from opentelemetry import trace
from starlette.datastructures import Headers

from app.config import get_config
from app.dependencies import X_WREN_FALLBACK_DISABLE, get_wren_headers, verify_query_dto
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
from app.util import (
    append_fallback_context,
    build_context,
    pushdown_limit,
    safe_strtobool,
    to_json,
)

router = APIRouter(prefix="/connector", tags=["connector"])
tracer = trace.get_tracer(__name__)


@router.post(
    "/{data_source}/query",
    dependencies=[Depends(verify_query_dto)],
    description="query the specified data source",
)
async def query(
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
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
    query_cache_manager: QueryCacheManager = Depends(get_query_cache_manager),
) -> Response:
    span_name = f"v3_query_{data_source}"
    if dry_run:
        span_name += "_dry_run"
    if cache_enable:
        span_name += "_cache_enable"

    is_fallback_disable = bool(
        headers.get(X_WREN_FALLBACK_DISABLE)
        and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
    )

    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        try:
            if dry_run:
                sql = pushdown_limit(dto.sql, limit)
                rewritten_sql = await Rewriter(
                    dto.manifest_str, data_source=data_source, experiment=True
                ).rewrite(sql)
                connector = Connector(data_source, dto.connection_info)
                connector.dry_run(rewritten_sql)
                return Response(status_code=204)

            # Not a dry run
            # Check if the query is cached
            cached_result = None
            cache_hit = False

            if cache_enable:
                cached_result = query_cache_manager.get(
                    data_source, dto.sql, dto.connection_info
                )
                cache_hit = cached_result is not None
            # case 1: cache hit read
            # We also need to check `is_fallback_disable` because a previous request
            # might have fallen back successfully  and created a cache.
            # If the current request has fallback disabled, we should bypass the cache hit check.
            if (
                cache_enable
                and cache_hit
                and not override_cache
                and not is_fallback_disable
            ):
                span.add_event("cache hit")
                response = ORJSONResponse(to_json(cached_result))
                response.headers["X-Cache-Hit"] = "true"
                response.headers["X-Cache-Create-At"] = str(
                    query_cache_manager.get_cache_file_timestamp(
                        data_source, dto.sql, dto.connection_info
                    )
                )
            # all other cases require rewriting + connecting
            else:
                sql = pushdown_limit(dto.sql, limit)
                rewritten_sql = await Rewriter(
                    dto.manifest_str, data_source=data_source, experiment=True
                ).rewrite(sql)
                connector = Connector(data_source, dto.connection_info)
                result = connector.query(rewritten_sql, limit=limit)
                response = ORJSONResponse(to_json(result))

                # headers for all non-hit cases
                response.headers["X-Cache-Hit"] = "false"

                match (cache_enable, cache_hit, override_cache):
                    # case 2: override existing cache
                    case (True, True, True):
                        response.headers["X-Cache-Create-At"] = str(
                            query_cache_manager.get_cache_file_timestamp(
                                data_source, dto.sql, dto.connection_info
                            )
                        )
                        query_cache_manager.set(
                            data_source, dto.sql, result, dto.connection_info
                        )

                        response.headers["X-Cache-Override"] = "true"
                        response.headers["X-Cache-Override-At"] = str(
                            query_cache_manager.get_cache_file_timestamp(
                                data_source, dto.sql, dto.connection_info
                            )
                        )
                    # case 3/4: cache miss but enabled (need to create cache)
                    # no matter the cache override or not, we need to create cache
                    case (True, False, _):
                        query_cache_manager.set(
                            data_source, dto.sql, result, dto.connection_info
                        )
                    # case 5~8 Other cases (cache is not enabled)
                    case (False, _, _):
                        pass

            return response
        except Exception as e:
            if is_fallback_disable:
                raise e

            logger.warning(
                "Failed to execute v3 query, try to fallback to v2: {}\n", str(e)
            )
            headers = append_fallback_context(headers, span)
            return await v2.connector.query(
                data_source=data_source,
                dto=dto,
                dry_run=dry_run,
                cache_enable=cache_enable,
                override_cache=override_cache,
                limit=limit,
                java_engine_connector=java_engine_connector,
                query_cache_manager=query_cache_manager,
                headers=headers,
                is_fallback=True,
            )


@router.post("/dry-plan", description="get the planned WrenSQL")
async def dry_plan(
    headers: Annotated[Headers, Depends(get_wren_headers)],
    dto: DryPlanDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> str:
    with tracer.start_as_current_span(
        name="dry_plan", kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        try:
            return await Rewriter(dto.manifest_str, experiment=True).rewrite(dto.sql)
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            if is_fallback_disable:
                raise e

            logger.warning(
                "Failed to execute v3 dry-plan, try to fallback to v2: {}", str(e)
            )
            headers = append_fallback_context(headers, span)
            return await v2.connector.dry_plan(
                dto=dto,
                java_engine_connector=java_engine_connector,
                headers=headers,
                is_fallback=True,
            )


@router.post(
    "/{data_source}/dry-plan",
    description="get the dialect SQL for the specified data source",
)
async def dry_plan_for_data_source(
    headers: Annotated[Headers, Depends(get_wren_headers)],
    data_source: DataSource,
    dto: DryPlanDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> str:
    span_name = f"v3_dry_plan_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        try:
            return await Rewriter(
                dto.manifest_str, data_source=data_source, experiment=True
            ).rewrite(dto.sql)
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            if is_fallback_disable:
                raise e

            logger.warning(
                "Failed to execute v3 dry-plan, try to fallback to v2: {}",
                str(e),
            )
            headers = append_fallback_context(headers, span)
            return await v2.connector.dry_plan_for_data_source(
                data_source=data_source,
                dto=dto,
                java_engine_connector=java_engine_connector,
                headers=headers,
                is_fallback=True,
            )


@router.post(
    "/{data_source}/validate/{rule_name}", description="validate the specified rule"
)
async def validate(
    headers: Annotated[Headers, Depends(get_wren_headers)],
    data_source: DataSource,
    rule_name: str,
    dto: ValidateDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> Response:
    span_name = f"v3_validate_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        try:
            validator = Validator(
                Connector(data_source, dto.connection_info),
                Rewriter(dto.manifest_str, data_source=data_source, experiment=True),
            )
            await validator.validate(rule_name, dto.parameters, dto.manifest_str)
            return Response(status_code=204)
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            if is_fallback_disable:
                raise e

            logger.warning(
                "Failed to execute v3 validate, try to fallback to v2: {}",
                str(e),
            )
            headers = append_fallback_context(headers, span)
            return await v2.connector.validate(
                data_source=data_source,
                rule_name=rule_name,
                dto=dto,
                java_engine_connector=java_engine_connector,
                headers=headers,
                is_fallback=True,
            )


@router.get(
    "/{data_source}/functions",
    description="get the available function list of the specified data source",
)
def functions(
    data_source: DataSource,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
) -> Response:
    span_name = f"v3_functions_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ):
        file_path = get_config().get_remote_function_list_path(data_source)
        session_context = get_session_context(None, file_path)
        func_list = [f.to_dict() for f in session_context.get_available_functions()]
        return ORJSONResponse(func_list)


@router.post(
    "/{data_source}/model-substitute",
    description="get the SQL which table name is substituted",
)
async def model_substitute(
    data_source: DataSource,
    dto: TranspileDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)],
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> str:
    span_name = f"v3_model-substitute_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        try:
            sql = ModelSubstitute(data_source, dto.manifest_str, headers).substitute(
                dto.sql
            )
            Connector(data_source, dto.connection_info).dry_run(
                await Rewriter(
                    dto.manifest_str,
                    data_source=data_source,
                    experiment=True,
                ).rewrite(sql)
            )
            return sql
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            if is_fallback_disable:
                raise e

            logger.warning(
                "Failed to execute v3 model-substitute, try to fallback to v2: {}",
                str(e),
            )
            headers = append_fallback_context(headers, span)
            return await v2.connector.model_substitute(
                data_source=data_source,
                dto=dto,
                headers=headers,
                java_engine_connector=java_engine_connector,
                is_fallback=True,
            )
