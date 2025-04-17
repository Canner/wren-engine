from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import ORJSONResponse
from loguru import logger
from opentelemetry import trace
from starlette.datastructures import Headers

from app.config import get_config
from app.dependencies import get_wren_headers, verify_query_dto
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

router = APIRouter(prefix="/connector", tags=["connector"])
tracer = trace.get_tracer(__name__)

MIGRATION_MESSAGE = "Wren engine is migrating to Rust version now. \
    Wren AI team are appreciate if you can provide the error messages and related logs for us."


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
                    data_source, dto.sql, dto.connection_info
                )
                cache_hit = cached_result is not None

            match (cache_enable, cache_hit, override_cache):
                # case 1 cache hit read
                case (True, True, False):
                    span.add_event("cache hit")
                    response = ORJSONResponse(to_json(cached_result))
                    response.headers["X-Cache-Hit"] = "true"
                    response.headers["X-Cache-Create-At"] = str(
                        query_cache_manager.get_cache_file_timestamp(
                            data_source, dto.sql, dto.connection_info
                        )
                    )
                # case 2 cache hit but override cache
                case (True, True, True):
                    result = connector.query(rewritten_sql, limit=limit)
                    response = ORJSONResponse(to_json(result))
                    # because we override the cache, so we need to set the cache hit to false
                    response.headers["X-Cache-Hit"] = "false"
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
                # case 3 and  case 4 cache miss read (first time cache read need to create cache)
                # no matter the cache override or not, we need to create cache
                case (True, False, _):
                    result = connector.query(rewritten_sql, limit=limit)

                    # set cache
                    query_cache_manager.set(
                        data_source, dto.sql, result, dto.connection_info
                    )
                    response = ORJSONResponse(to_json(result))
                    response.headers["X-Cache-Hit"] = "false"
                # case 5~8 Other cases (cache is not enabled)
                case (False, _, _):
                    result = connector.query(rewritten_sql, limit=limit)
                    response = ORJSONResponse(to_json(result))
                    response.headers["X-Cache-Hit"] = "false"

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
                override_cache,
                limit,
                java_engine_connector,
                query_cache_manager,
                headers,
            )


@router.post("/dry-plan", description="get the planned WrenSQL")
async def dry_plan(
    dto: DryPlanDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
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


@router.post(
    "/{data_source}/dry-plan",
    description="get the dialect SQL for the specified data source",
)
async def dry_plan_for_data_source(
    data_source: DataSource,
    dto: DryPlanDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
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


@router.post(
    "/{data_source}/validate/{rule_name}", description="validate the specified rule"
)
async def validate(
    data_source: DataSource,
    rule_name: str,
    dto: ValidateDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
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
    ):
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
            logger.warning(
                "Failed to execute v3 model-substitute, fallback to v2: {}", str(e)
            )
            return await v2.connector.model_substitute(
                data_source, dto, headers, java_engine_connector
            )
