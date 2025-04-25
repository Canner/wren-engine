import hashlib
import time
from typing import Any, Optional

import ibis
import opendal
from loguru import logger
from opentelemetry import trace

tracer = trace.get_tracer(__name__)


class QueryCacheManager:
    def __init__(self, root: str = "/tmp/wren-engine/"):
        self.root = root

    @tracer.start_as_current_span("get_cache", kind=trace.SpanKind.INTERNAL)
    def get(self, data_source: str, sql: str, info) -> Optional[Any]:
        cache_key = self._generate_cache_key(data_source, sql, info)
        cache_file_name = self._get_cache_file_name(cache_key)
        op = self._get_dal_operator()
        full_path = self._get_full_path(cache_file_name)

        # Check if cache file exists
        if op.exists(cache_file_name):
            try:
                logger.info(f"\nReading query cache {cache_file_name}\n")
                cache = ibis.read_parquet(full_path)
                df = cache.execute()
                logger.info("\nquery cache to dataframe\n")
                return df
            except Exception as e:
                logger.debug(f"Failed to read query cache {e}")
                return None

        return None

    @tracer.start_as_current_span("set_cache", kind=trace.SpanKind.INTERNAL)
    def set(self, data_source: str, sql: str, result: Any, info) -> None:
        cache_key = self._generate_cache_key(data_source, sql, info)
        cache_file_name = self._set_cache_file_name(cache_key)
        op = self._get_dal_operator()
        full_path = self._get_full_path(cache_file_name)

        try:
            # Create cache directory if it doesn't exist
            with op.open(cache_file_name, mode="wb") as file:
                cache = ibis.memtable(result)
                logger.info(f"\nWriting query cache to {cache_file_name}\n")
                if file.writable():
                    cache.to_parquet(full_path)
        except Exception as e:
            logger.debug(f"Failed to write query cache: {e}")
            return

    def get_cache_file_timestamp(self, data_source: str, sql: str, info) -> int | None:
        cache_key = self._generate_cache_key(data_source, sql, info)
        op = self._get_dal_operator()
        for file in op.list("/"):
            if file.path.startswith(cache_key):
                # xxxxxxxxxxxxxx-1744016574.cache
                # we only care about the timestamp part
                try:
                    timestamp = int(file.path.split("-")[-1].split(".")[0])
                    return timestamp
                except (IndexError, ValueError) as e:
                    logger.debug(
                        f"Failed to extract timestamp from cache file {file.path}: {e}"
                    )
        return None

    def _generate_cache_key(self, data_source: str, sql: str, info) -> str:
        connection_key = info.to_key_string()

        # Combine with data source and SQL
        key_string = f"{data_source}|{sql}|{connection_key}"

        return hashlib.sha256(key_string.encode()).hexdigest()

    def _get_cache_file_name(self, cache_key: str) -> str:
        op = self._get_dal_operator()
        for file in op.list("/"):
            if file.path.startswith(cache_key):
                return file.path

        cache_create_timestamp = int(time.time() * 1000)
        return f"{cache_key}-{cache_create_timestamp}.cache"

    def _set_cache_file_name(self, cache_key: str) -> str:
        # Delete old cache files, make only one cache file per query
        op = self._get_dal_operator()
        for file in op.list("/"):
            if file.path.startswith(cache_key):
                logger.info(f"Deleting old cache file {file.path}")
                op.delete(file.path)

        cache_create_timestamp = int(time.time() * 1000)
        return f"{cache_key}-{cache_create_timestamp}.cache"

    def _get_full_path(self, path: str) -> str:
        return self.root + path

    def _get_dal_operator(self) -> Any:
        # Default implementation using local filesystem
        return opendal.Operator("fs", root=self.root)
