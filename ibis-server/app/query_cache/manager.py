import hashlib
import time
from typing import Any, Optional

import opendal
import pyarrow as pa
from duckdb import DuckDBPyConnection, connect
from loguru import logger

from app.dependencies import (
    X_WREN_DB_STATEMENT_TIMEOUT,
    X_WREN_FALLBACK_DISABLE,
    X_WREN_TIMEZONE,
    X_WREN_VARIABLE_PREFIX,
)


class QueryCacheImpl:
    def __init__(self, root: str = "/tmp/wren-engine/"):
        self.root = root

    def get(
        self,
        data_source: str,
        sql: str,
        info,
        headers: Optional[dict[str, str]] = None,
    ) -> "Optional[Any]":
        cache_key = self._generate_cache_key(data_source, sql, info, headers)
        cache_file_name = self._get_cache_file_name(cache_key)

        op = self._get_dal_operator()
        full_path = self._get_full_path(cache_file_name)

        # Check if cache file exists
        if op.exists(cache_file_name):
            try:
                logger.info("Reading query cache {}", full_path)
                con = self._get_duckdb_connection()
                cache = con.read_parquet(full_path)
                df = cache.to_arrow_table()
                logger.info("query cache to dataframe")
                return df
            except Exception as e:
                logger.debug("Failed to read query cache {}", e)
                return None

        return None

    def set(
        self,
        data_source: str,
        sql: str,
        result: pa.Table,
        info,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        cache_key = self._generate_cache_key(data_source, sql, info, headers)
        cache_file_name = self._set_cache_file_name(cache_key)
        op = self._get_dal_operator()
        full_path = self._get_full_path(cache_file_name)
        try:
            # Create cache directory if it doesn't exist
            with op.open(cache_file_name, mode="wb") as file:
                logger.info("Writing query cache to {}", full_path)
                con = self._get_duckdb_connection()
                arrow_table = con.from_arrow(result)
                if file.writable():
                    arrow_table.write_parquet(full_path)
        except Exception as e:
            logger.debug("Failed to write query cache: {}", e)
            return

    def get_cache_file_timestamp(
        self,
        data_source: str,
        sql: str,
        info,
        headers: Optional[dict[str, str]] = None,
    ) -> int | None:
        cache_key = self._generate_cache_key(data_source, sql, info, headers)
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

    def _generate_cache_key(
        self, data_source: str, sql: str, info, headers: dict[str, str] | None = None
    ) -> str:
        connection_key = info.to_key_string()

        # Create a normalized headers string for cache key
        headers_key = self._normalize_headers_for_cache(headers)

        # Combine with data source, SQL, connection info, and headers
        key_string = f"{data_source}|{sql}|{connection_key}|{headers_key}"

        return hashlib.sha256(key_string.encode()).hexdigest()

    def _normalize_headers_for_cache(
        self, headers: dict[str, str] | None = None
    ) -> str:
        if not headers:
            return ""

        # Define which headers should be included in cache key
        # These are headers that can affect the query results
        cache_relevant_headers = [
            X_WREN_VARIABLE_PREFIX,
            X_WREN_FALLBACK_DISABLE,
            X_WREN_TIMEZONE,
            X_WREN_DB_STATEMENT_TIMEOUT,
        ]

        # Filter headers that are relevant for caching
        relevant_headers = {}
        for key, value in headers.items():
            key_lower = key.lower()
            for relevant_prefix in cache_relevant_headers:
                if key_lower.startswith(relevant_prefix):
                    relevant_headers[key_lower] = str(value)
                    break

        # Sort headers by key to ensure consistent ordering
        sorted_headers = sorted(relevant_headers.items())

        # Create a string representation
        headers_str = "|".join([f"{k}:{v}" for k, v in sorted_headers])

        return headers_str

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

    def _get_duckdb_connection(self) -> DuckDBPyConnection:
        con = connect()
        _set_utc_timezone(con)
        return con


def _set_utc_timezone(con: DuckDBPyConnection) -> None:
    try:
        con.execute("SET TimeZone = 'UTC'")
    except Exception as e:
        logger.error("Failed to set UTC timezone: {}", e)
        raise
