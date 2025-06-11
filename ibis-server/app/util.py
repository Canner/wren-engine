import base64
import datetime
import decimal

import orjson
import pandas as pd
import pyarrow as pa
import wren_core
from fastapi import Header
from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context
from opentelemetry.propagate import extract
from opentelemetry.trace import (
    NonRecordingSpan,
    set_span_in_context,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pandas.core.dtypes.common import is_datetime64_any_dtype
from starlette.datastructures import Headers

from app.model.data_source import DataSource

tracer = trace.get_tracer(__name__)


MIGRATION_MESSAGE = "Wren engine is migrating to Rust version now. \
    Wren AI team are appreciate if you can provide the error messages and related logs for us."


@tracer.start_as_current_span("base64_to_dict", kind=trace.SpanKind.INTERNAL)
def base64_to_dict(base64_str: str) -> dict:
    return orjson.loads(base64.b64decode(base64_str).decode("utf-8"))


@tracer.start_as_current_span("to_json", kind=trace.SpanKind.INTERNAL)
def to_json(df: pd.DataFrame) -> dict:
    original_dtype = df.dtypes.map(
        lambda x: str(x.pyarrow_dtype) if hasattr(x, "pyarrow_dtype") else str(x)
    ).to_dict()
    for column in df.columns:
        if _is_arrow_datetime(df[column]) and is_datetime64_any_dtype(df[column].dtype):
            df[column] = _to_datetime_and_format(df[column])
    json_obj = _to_json_obj(df)
    json_obj["dtypes"] = original_dtype
    return json_obj


def _is_arrow_datetime(series: pd.Series) -> bool:
    dtype = series.dtype
    if hasattr(dtype, "pyarrow_dtype"):
        pa_type = dtype.pyarrow_dtype
        return pa.types.is_timestamp(pa_type)
    return False


def _to_datetime_and_format(series: pd.Series) -> pd.Series:
    return series.apply(
        lambda d: d.strftime(
            "%Y-%m-%d %H:%M:%S.%f" + (" %Z" if series.dt.tz is not None else "")
        )
        if not pd.isnull(d)
        else d
    )


def _to_json_obj(df: pd.DataFrame) -> dict:
    def format_value(x):
        # Need to handle NaN first, as it can be a float or pd.NA
        if pd.isna(x):
            return None
        elif isinstance(x, float):
            return f"{x:.9g}"
        elif isinstance(x, decimal.Decimal):
            if x == 0:
                return "0"
            else:
                return x
        else:
            return x

    data = df.map(format_value).to_dict(orient="split", index=False)

    def default(obj):
        if pd.isna(obj):
            return None
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()
        if isinstance(obj, pd.tseries.offsets.DateOffset):
            return _date_offset_to_str(obj)
        if isinstance(obj, datetime.timedelta):
            return str(obj)
        # Add handling for any remaining LOB objects
        if hasattr(obj, "read"):  # Check if object is LOB-like
            return str(obj)
        raise TypeError

    json_obj = orjson.loads(
        orjson.dumps(
            data,
            option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_SERIALIZE_UUID,
            default=default,
        )
    )
    return json_obj


def _date_offset_to_str(offset: pd.tseries.offsets.DateOffset) -> str:
    parts = []
    units = [
        "months",
        "days",
        "microseconds",
        "nanoseconds",
    ]

    for unit in units:
        value = getattr(offset, unit, 0)
        if value:
            parts.append(f"{value} {unit if value > 1 else unit.rstrip('s')}")

    return " ".join(parts)


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
