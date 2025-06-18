import base64

import duckdb
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
def to_json(df: pa.Table, headers: dict) -> dict:
    dtypes = {field.name: str(field.type) for field in df.schema}
    if df.num_rows == 0:
        return {
            "columns": [field.name for field in df.schema],
            "data": [],
            "dtypes": dtypes,
        }

    formatted_sql = (
        "SELECT " + ", ".join([_formater(field) for field in df.schema]) + " FROM df"
    )
    logger.debug(f"formmated_sql: {formatted_sql}")
    conn = get_duckdb_conn(headers)
    formatted_df = conn.execute(formatted_sql).fetch_df()

    result = formatted_df.to_dict(orient="split")
    result["dtypes"] = dtypes
    result.pop("index", None)  # Remove index field from the DuckDB result
    return result


def get_duckdb_conn(headers: dict) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection with the provided headers."""
    conn = duckdb.connect()
    if X_WREN_TIMEZONE in headers:
        timezone = headers[X_WREN_TIMEZONE]
        if timezone.startwith("+") or timezone.startswith("-"):
            # If the timezone is an offset, convert it to a named timezone
            timezone = get_timezone_from_offset(timezone)
        conn.execute("SET TimeZone = ?", [timezone])
    else:
        # Default to UTC if no timezone is provided
        conn.execute("SET TimeZone = 'UTC'")

    return conn


def get_timezone_from_offset(offset: str) -> str:
    if offset.startswith("+"):
        offset = offset[1:]  # Remove the leading '+' sign

    first = duckdb.execute(
        "SELECT name, utc_offset FROM pg_timezone_names() WHERE utc_offset = ?",
        [offset],
    ).fetchone()
    if first is None:
        raise ValueError(f"Invalid timezone offset: {offset}")
    return first[0]  # Return the timezone name


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


def _formater(field: pa.Field) -> str:
    column_name = _quote_identifier(field.name)
    if pa.types.is_decimal(field.type) or pa.types.is_floating(field.type):
        return f"""
        case when {column_name} = 0 then '0'
        when length(CAST({column_name} AS VARCHAR)) > 15 then format('{{:.9g}}', {column_name})
        else RTRIM(RTRIM(format('{{:.8f}}', {column_name}), '0'), '.')
        end as {column_name}"""
    elif pa.types.is_date(field.type):
        return f"strftime({column_name}, '%Y-%m-%d') as {column_name}"
    elif pa.types.is_timestamp(field.type):
        if field.type.tz is None:
            return f"strftime({column_name}, '%Y-%m-%d %H:%M:%S.%f') as {column_name}"
        else:
            return (
                f"strftime({column_name}, '%Y-%m-%d %H:%M:%S.%f %Z') as {column_name}"
            )
    elif pa.types.is_binary(field.type):
        return f"to_hex({column_name}) as {column_name}"
    elif pa.types.is_interval(field.type):
        return f"cast({column_name} as varchar) as {column_name}"
    return column_name


def _quote_identifier(identifier: str) -> str:
    identifier = identifier.replace('"', '""')  # Escape double quotes
    return f'"{identifier}"' if identifier else identifier
