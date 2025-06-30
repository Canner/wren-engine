import base64

import datafusion
import orjson
import pandas as pd
import pyarrow as pa
import wren_core
from fastapi import Header
from loguru import logger
from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context
from opentelemetry.propagate import extract
from opentelemetry.trace import (
    NonRecordingSpan,
    set_span_in_context,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from starlette.datastructures import Headers

from app.dependencies import (
    X_CACHE_CREATE_AT,
    X_CACHE_HIT,
    X_CACHE_OVERRIDE,
    X_CACHE_OVERRIDE_AT,
    X_WREN_TIMEZONE,
)
from app.model.data_source import DataSource

tracer = trace.get_tracer(__name__)


MIGRATION_MESSAGE = "Wren engine is migrating to Rust version now. \
    Wren AI team are appreciate if you can provide the error messages and related logs for us."


@tracer.start_as_current_span("base64_to_dict", kind=trace.SpanKind.INTERNAL)
def base64_to_dict(base64_str: str) -> dict:
    return orjson.loads(base64.b64decode(base64_str).decode("utf-8"))


@tracer.start_as_current_span("to_json", kind=trace.SpanKind.INTERNAL)
def to_json(df: pa.Table, headers: dict, data_source: DataSource = None) -> dict:
    df = _with_session_timezone(df, headers, data_source)
    dtypes = {field.name: str(field.type) for field in df.schema}
    if df.num_rows == 0:
        return {
            "columns": [field.name for field in df.schema],
            "data": [],
            "dtypes": dtypes,
        }

    ctx = get_datafusion_context(headers)
    ctx.register_record_batches(name="arrow_table", partitions=[df.to_batches()])

    formatted_sql = (
        "SELECT "
        + ", ".join([_formater(field) for field in df.schema])
        + " FROM arrow_table"
    )
    logger.debug(f"formmated_sql: {formatted_sql}")
    formatted_df = ctx.sql(formatted_sql).to_pandas()

    result = formatted_df.to_dict(orient="split")
    result["dtypes"] = dtypes
    result.pop("index", None)  # Remove index field from the DuckDB result
    return result


def _with_session_timezone(
    df: pa.Table, headers: dict, data_source: DataSource
) -> pa.Table:
    fields = []

    for field in df.schema:
        if pa.types.is_timestamp(field.type):
            if field.type.tz is not None and X_WREN_TIMEZONE in headers:
                # change the timezone to the seesion timezone
                fields.append(
                    pa.field(
                        field.name,
                        pa.timestamp(field.type.unit, tz=headers[X_WREN_TIMEZONE]),
                        nullable=True,
                    )
                )
                continue
            if data_source == DataSource.mysql:
                timezone = headers.get(X_WREN_TIMEZONE, "UTC")
                # TODO: ibis mysql loss the timezone information
                # we cast timestamp to timestamp with session timezone for mysql
                fields.append(
                    pa.field(
                        field.name,
                        pa.timestamp(field.type.unit, tz=timezone),
                        nullable=True,
                    )
                )
                continue

        # TODO: the field's nullable should be Ture if the value contains null but
        # the arrow table produced by the ibis clickhouse connector always set nullable to False
        # so we set nullable to True here to avoid the casting error
        fields.append(
            pa.field(
                field.name,
                field.type,
                nullable=True,
            )
        )
    return df.cast(pa.schema(fields))


def get_datafusion_context(headers: dict) -> datafusion.SessionContext:
    config = datafusion.SessionConfig()
    if X_WREN_TIMEZONE in headers:
        config.set("datafusion.execution.time_zone", headers[X_WREN_TIMEZONE])
    else:
        # Default to UTC if no timezone is provided
        config.set("datafusion.execution.time_zone", "UTC")

    ctx = datafusion.SessionContext(config=config)
    return ctx


def build_context(headers: Header) -> Context:
    if headers is None:
        return None
    return extract(headers)


def set_attribute(
    header: Header,
    span: trace.Span,
) -> None:
    if header is None:
        return
    if "X-Correlation-ID" in header:
        span.set_attribute("correlation_id", header["X-Correlation-ID"])


def append_fallback_context(headers: Header, span: trace.Span) -> Headers:
    if headers is None:
        headers = {}
    else:
        headers = dict(headers)
    span = NonRecordingSpan(span.get_span_context())
    context = set_span_in_context(span)
    # https://opentelemetry.io/docs/languages/python/propagation/
    W3CBaggagePropagator().inject(headers, context)
    TraceContextTextMapPropagator().inject(headers, context)
    return Headers(headers)


@tracer.start_as_current_span("pushdown_limit", kind=trace.SpanKind.INTERNAL)
def pushdown_limit(sql: str, limit: int | None) -> str:
    ctx = wren_core.SessionContext()
    return ctx.pushdown_limit(sql, limit)


def get_fallback_message(
    logger, prefix: str, datasource: DataSource, mdl_base64: str, sql: str
) -> str:
    if sql is not None:
        sql = sql.replace("\n", " ")

    message = orjson.dumps(
        {"datasource": datasource, "mdl_base64": mdl_base64, "sql": sql}
    ).decode("utf-8")
    logger.warning("Fallback to v2 {} -- {}\n{}", prefix, message, MIGRATION_MESSAGE)  # noqa: PLE1205


def safe_strtobool(val: str) -> bool:
    return val.lower() in {"1", "true", "yes", "y"}


def pd_to_arrow_schema(df: pd.DataFrame) -> pa.Schema:
    fields = []
    for column in df.columns:
        dtype = df[column].dtype
        if hasattr(dtype, "pyarrow_dtype"):
            pa_type = dtype.pyarrow_dtype
        else:
            # Fallback to string type for unsupported dtypes
            pa_type = pa.string()
        fields.append(pa.field(column, pa_type))
    return pa.schema(fields)


def update_response_headers(response, required_headers: dict):
    if X_CACHE_HIT in required_headers:
        response.headers[X_CACHE_HIT] = required_headers[X_CACHE_HIT]
    if X_CACHE_CREATE_AT in required_headers:
        response.headers[X_CACHE_CREATE_AT] = required_headers[X_CACHE_CREATE_AT]
    if X_CACHE_OVERRIDE in required_headers:
        response.headers[X_CACHE_OVERRIDE] = required_headers[X_CACHE_OVERRIDE]
    if X_CACHE_OVERRIDE_AT in required_headers:
        response.headers[X_CACHE_OVERRIDE_AT] = required_headers[X_CACHE_OVERRIDE_AT]


def _quote_identifier(identifier: str) -> str:
    identifier = identifier.replace('"', '""')  # Escape double quotes
    return f'"{identifier}"' if identifier else identifier


def _formater(field: pa.Field) -> str:
    column_name = _quote_identifier(field.name)
    if pa.types.is_decimal(field.type):
        # TODO: maybe implement a to_char udf to fomrat decimal would be better
        # Currently, if the nubmer is less than 1, it will show with exponential notation if the lenth of float digits is great than 7
        # e.g. 0.0000123 will be shown without exponential notation but 0.0000123 will be shown with exponential notation 1.23e-6
        return f"case when {column_name} = 0 then '0' else cast({column_name} as double) end as {column_name}"
    elif pa.types.is_date(field.type):
        return f"to_char({column_name}, '%Y-%m-%d') as {column_name}"
    elif pa.types.is_timestamp(field.type):
        if field.type.tz is None:
            return f"to_char({column_name}, '%Y-%m-%d %H:%M:%S%.6f') as {column_name}"
        else:
            return (
                f"to_char({column_name}, '%Y-%m-%d %H:%M:%S%.6f %Z') as {column_name}"
            )
    elif pa.types.is_binary(field.type):
        return f"encode({column_name}, 'hex') as {column_name}"
    elif pa.types.is_interval(field.type):
        return f"cast({column_name} as varchar) as {column_name}"
    return column_name
