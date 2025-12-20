from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import ORJSONResponse
from loguru import logger
from opentelemetry import trace
from starlette.datastructures import Headers

from app.config import get_config
from app.dependencies import (
    X_CACHE_CREATE_AT,
    X_CACHE_HIT,
    X_CACHE_OVERRIDE,
    X_CACHE_OVERRIDE_AT,
    X_WREN_FALLBACK_DISABLE,
    get_wren_headers,
    is_backward_compatible,
    verify_query_dto,
)
from app.mdl import knowledge
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
from app.model.error import DatabaseTimeoutError
from app.model.metadata.dto import Catalog, MetadataDTO, Table, get_filter_info
from app.model.metadata.factory import MetadataFactory
from app.model.validator import Validator
from app.query_cache import QueryCacheManager
from app.routers import v2
from app.routers.v2.connector import get_java_engine_connector, get_query_cache_manager
from app.util import (
    append_fallback_context,
    build_context,
    execute_dry_run_with_timeout,
    execute_get_schema_list_with_timeout,
    execute_get_table_list_with_timeout,
    execute_query_with_timeout,
    execute_validate_with_timeout,
    pushdown_limit,
    safe_strtobool,
    set_attribute,
    to_json,
    update_response_headers,
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

    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        # Convert headers to dict for cache manager
        headers_dict = dict(headers) if headers else None

        try:
            if dry_run:
                sql = pushdown_limit(dto.sql, limit)
                rewritten_sql = await Rewriter(
                    dto.manifest_str,
                    data_source=data_source,
                    experiment=True,
                    properties=dict(headers),
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
                sql = pushdown_limit(dto.sql, limit)
                rewritten_sql = await Rewriter(
                    dto.manifest_str,
                    data_source=data_source,
                    experiment=True,
                    properties=dict(headers),
                ).rewrite(sql)
                connector = Connector(data_source, connection_info)
                result = await execute_query_with_timeout(
                    connector,
                    rewritten_sql,
                )

                # headers for all non-hit cases
                cache_headers[X_CACHE_HIT] = "false"

                if cache_enable and cache_hit and override_cache:
                    # case 2: override existing cache
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
                elif cache_enable and not cache_hit:
                    # case 3/4: cache miss but enabled (need to create cache)
                    # no matter the cache override or not, we need to create cache
                    query_cache_manager.set(
                        data_source,
                        dto.sql,
                        result,
                        connection_info,
                        headers_dict,
                    )
                elif not cache_enable:
                    # case 5~8 Other cases (cache is not enabled)
                    pass

            response = ORJSONResponse(to_json(result, headers, data_source=data_source))
            update_response_headers(response, cache_headers)
            return response
        except DatabaseTimeoutError:
            # won't fallback to v2 if timeout
            raise
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            # because the v2 API doesn't support row-level access control,
            # we don't fallback to v2 if the header include row-level access control properties.
            if (
                java_engine_connector.client is None
                or is_fallback_disable
                or not is_backward_compatible(dto.manifest_str)
            ):
                raise e

            logger.warning(
                "Failed to execute v3 query, try to fallback to v2: {}\n", str(e)
            )
            headers = append_fallback_context(headers, span)
            try:
                return await v2.connector.query(
                    data_source=data_source,
                    dto=dto,
                    dry_run=dry_run,
                    limit=limit,
                    java_engine_connector=java_engine_connector,
                    headers=headers,
                    is_fallback=True,
                    cache_enable=cache_enable,
                    override_cache=override_cache,
                    query_cache_manager=query_cache_manager,
                )
            except Exception as ve:
                # ignore v2 error messages in fallback, return v3 error instead.
                logger.debug(
                    "v2 fallback failed for v3 query; suppressing v2 error: %s", ve
                )
                raise e from None


@router.post("/dry-plan", description="get the planned WrenSQL")
async def dry_plan(
    headers: Annotated[Headers, Depends(get_wren_headers)],
    dto: DryPlanDTO,
    java_engine_connector: JavaEngineConnector = Depends(get_java_engine_connector),
) -> str:
    with tracer.start_as_current_span(
        name="dry_plan", kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        try:
            return await Rewriter(
                dto.manifest_str, experiment=True, properties=dict(headers)
            ).rewrite(dto.sql)
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            # because the v2 API doesn't support row-level access control,
            # we don't fallback to v2 if the header include row-level access control properties.
            if (
                java_engine_connector.client is None
                or is_fallback_disable
                or not is_backward_compatible(dto.manifest_str)
            ):
                raise e

            logger.warning(
                "Failed to execute v3 dry-plan, try to fallback to v2: {}", str(e)
            )
            headers = append_fallback_context(headers, span)
            try:
                return await v2.connector.dry_plan(
                    dto=dto,
                    java_engine_connector=java_engine_connector,
                    headers=headers,
                    is_fallback=True,
                )
            except Exception as ve:
                # ignore v2 error messages in fallback, return v3 error instead.
                logger.debug(
                    "v2 fallback failed for v3 dry-plan; suppressing v2 error: %s", ve
                )
                raise e from None


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
        set_attribute(headers, span)
        try:
            return await Rewriter(
                dto.manifest_str,
                data_source=data_source,
                experiment=True,
                properties=dict(headers),
            ).rewrite(dto.sql)
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            # because the v2 API doesn't support row-level access control,
            # we don't fallback to v2 if the header include row-level access control properties.
            if (
                java_engine_connector.client is None
                or is_fallback_disable
                or not is_backward_compatible(dto.manifest_str)
            ):
                raise e

            logger.warning(
                "Failed to execute v3 dry-plan, try to fallback to v2: {}",
                str(e),
            )
            headers = append_fallback_context(headers, span)
            try:
                return await v2.connector.dry_plan_for_data_source(
                    data_source=data_source,
                    dto=dto,
                    java_engine_connector=java_engine_connector,
                    headers=headers,
                    is_fallback=True,
                )
            except Exception as ve:
                # ignore v2 error messages in fallback, return v3 error instead.
                logger.debug(
                    "v2 fallback failed for v3 dry-plan; suppressing v2 error: %s", ve
                )
                raise e from None


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
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        try:
            validator = Validator(
                Connector(data_source, connection_info),
                Rewriter(
                    dto.manifest_str,
                    data_source=data_source,
                    experiment=True,
                    properties=dict(headers),
                ),
            )
            await execute_validate_with_timeout(
                validator,
                rule_name,
                dto.parameters,
                dto.manifest_str,
            )
            return Response(status_code=204)
        except DatabaseTimeoutError:
            # won't fallback to v2 if timeout
            raise
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            # because the v2 API doesn't support row-level access control,
            # we don't fallback to v2 if the header include row-level access control properties.
            if (
                java_engine_connector.client is None
                or is_fallback_disable
                or not is_backward_compatible(dto.manifest_str)
            ):
                raise e

            logger.warning(
                "Failed to execute v3 validate, try to fallback to v2: {}",
                str(e),
            )
            headers = append_fallback_context(headers, span)
            try:
                return await v2.connector.validate(
                    data_source=data_source,
                    rule_name=rule_name,
                    dto=dto,
                    java_engine_connector=java_engine_connector,
                    headers=headers,
                    is_fallback=True,
                )
            except Exception as ve:
                # ignore v2 error messages in fallback, return v3 error instead.
                logger.debug(
                    "v2 fallback failed for v3 validate; suppressing v2 error: %s", ve
                )
                raise e from None


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
        white_function_list_path = get_config().get_remote_white_function_list_path(
            data_source
        )
        is_white_list = get_config().get_data_source_is_white_list(data_source)
        session_context = get_session_context(None, file_path)
        if is_white_list:
            func_list = (
                duckdb.read_csv(
                    white_function_list_path,
                    header=True,
                )
                .to_df()
                .to_dict("records")
            )
        else:
            func_list = [f.to_dict() for f in session_context.get_available_functions()]
        return ORJSONResponse(func_list)


@router.get(
    "/{data_source}/function/{function_name}",
    description="get the available function list of the specified data source",
)
def function(
    data_source: DataSource,
    function_name: str,
    headers: Annotated[Headers, Depends(get_wren_headers)] = None,
) -> Response:
    span_name = f"v3_get_function_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        file_path = get_config().get_remote_function_list_path(data_source)
        session_context = get_session_context(None, file_path)
        func_list = [
            f.to_dict() for f in session_context.get_available_function(function_name)
        ]
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
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        try:
            sql = ModelSubstitute(data_source, dto.manifest_str, headers).substitute(
                dto.sql
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
            return sql
        except DatabaseTimeoutError:
            # won't fallback to v2 if timeout
            raise
        except Exception as e:
            is_fallback_disable = bool(
                headers.get(X_WREN_FALLBACK_DISABLE)
                and safe_strtobool(headers.get(X_WREN_FALLBACK_DISABLE, "false"))
            )
            if (
                java_engine_connector.client is None
                or is_fallback_disable
                or not is_backward_compatible(dto.manifest_str)
            ):
                raise e

            logger.warning(
                "Failed to execute v3 model-substitute, try to fallback to v2: {}",
                str(e),
            )
            headers = append_fallback_context(headers, span)
            try:
                return await v2.connector.model_substitute(
                    data_source=data_source,
                    dto=dto,
                    headers=headers,
                    java_engine_connector=java_engine_connector,
                    is_fallback=True,
                )
            except Exception as ve:
                # ignore v2 error messages in fallback, return v3 error instead.
                logger.debug(
                    "v2 fallback failed for v3 model-substitute; suppressing v2 error: %s",
                    ve,
                )
                raise e from None


@router.get(
    "/{data_source}/knowledge",
    description="get the SQL knowledge of the specified data source",
)
async def get_sql_knowledge(
    data_source: DataSource,
):
    knowledge_manager = knowledge.Knowledge(data_source=data_source)
    return {
        "text_to_sql_rule": knowledge_manager.get_text_to_sql_rule(),
        "instructions": knowledge_manager.get_sql_instructions(),
        "sql_correction_rule": knowledge_manager.get_sql_correction_rule(),
    }


@router.post(
    "/{data_source}/metadata/tables",
    description="get the table metadata of the specified data source",
)
async def get_table_list(
    data_source: DataSource,
    dto: MetadataDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)],
) -> list[Table]:
    span_name = f"v3_get_table_list_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        metadata = MetadataFactory.get_metadata(data_source, connection_info)
        filter_info = get_filter_info(data_source, dto.filter_info or {})
        return await execute_get_table_list_with_timeout(
            metadata,
            filter_info,
            dto.table_limit,
        )


@router.post(
    "/{data_source}/metadata/schemas",
    description="get the schema metadata of the specified data source",
)
async def get_schema_list(
    data_source: DataSource,
    dto: MetadataDTO,
    headers: Annotated[Headers, Depends(get_wren_headers)],
) -> list[Catalog]:
    span_name = f"v3_get_schema_list_{data_source}"
    with tracer.start_as_current_span(
        name=span_name, kind=trace.SpanKind.SERVER, context=build_context(headers)
    ) as span:
        set_attribute(headers, span)
        connection_info = data_source.get_connection_info(
            dto.connection_info, dict(headers)
        )
        metadata = MetadataFactory.get_metadata(data_source, connection_info)
        filter_info = get_filter_info(data_source, dto.filter_info or {})
        return await execute_get_schema_list_with_timeout(
            metadata,
            filter_info,
            dto.table_limit,
        )
