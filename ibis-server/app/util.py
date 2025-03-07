import base64
import datetime
import decimal

import orjson
import pandas as pd
import wren_core
from fastapi import Header
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import extract
from pandas.core.dtypes.common import is_datetime64_any_dtype

tracer = trace.get_tracer(__name__)


@tracer.start_as_current_span("base64_to_dict", kind=trace.SpanKind.INTERNAL)
def base64_to_dict(base64_str: str) -> dict:
    return orjson.loads(base64.b64decode(base64_str).decode("utf-8"))


@tracer.start_as_current_span("to_json", kind=trace.SpanKind.INTERNAL)
def to_json(df: pd.DataFrame) -> dict:
    for column in df.columns:
        if is_datetime64_any_dtype(df[column].dtype):
            df[column] = _to_datetime_and_format(df[column])
    return _to_json_obj(df)


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
        if isinstance(x, float):
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
    json_obj["dtypes"] = df.dtypes.astype(str).to_dict()
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


@tracer.start_as_current_span("pushdown_limit", kind=trace.SpanKind.INTERNAL)
def pushdown_limit(sql: str, limit: int | None) -> str:
    ctx = wren_core.SessionContext()
    return ctx.pushdown_limit(sql, limit)
