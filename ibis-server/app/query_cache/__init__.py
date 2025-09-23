from typing import Any, Optional

import pyarrow as pa
from opentelemetry import trace

from app.query_cache.manager import QueryCacheImpl

tracer = trace.get_tracer(__name__)


class QueryCacheManager:
    def __init__(self, delegate: QueryCacheImpl = None):
        if delegate is None:
            self.delegate = QueryCacheImpl()
        else:
            self.delegate = delegate

    @tracer.start_as_current_span("get_cache", kind=trace.SpanKind.INTERNAL)
    def get(
        self,
        data_source: str,
        sql: str,
        info,
        headers: Optional[dict[str, str]] = None,
    ) -> "Optional[Any]":
        return self.delegate.get(data_source, sql, info, headers)

    @tracer.start_as_current_span("set_cache", kind=trace.SpanKind.INTERNAL)
    def set(
        self,
        data_source: str,
        sql: str,
        result: pa.Table,
        info,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.delegate.set(data_source, sql, result, info, headers)

    def get_cache_file_timestamp(
        self,
        data_source: str,
        sql: str,
        info,
        headers: Optional[dict[str, str]] = None,
    ) -> int | None:
        return self.delegate.get_cache_file_timestamp(data_source, sql, info, headers)
