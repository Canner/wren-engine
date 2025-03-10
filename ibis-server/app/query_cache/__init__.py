import hashlib
import os
from typing import Any, Optional

import ibis
from loguru import logger


class QueryCacheManager:
    def __init__(self):
        pass

    def get(self, data_source: str, sql: str, info) -> Optional[Any]:
        cache_key = self._generate_cache_key(data_source, sql, info)
        cache_path = self._get_cache_path(cache_key)

        # Check if cache file exists
        if os.path.exists(cache_path):
            try:
                cache = ibis.read_parquet(cache_path)
                df = cache.execute()
                return df
            except Exception as e:
                logger.debug(f"Failed to read query cache {e}")
                return None

        return None

    def set(self, data_source: str, sql: str, result: Any, info) -> None:
        cache_key = self._generate_cache_key(data_source, sql, info)
        cache_path = self._get_cache_path(cache_key)

        try:
            # Create cache directory if it doesn't exist
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            cache = ibis.memtable(result)
            cache.to_parquet(cache_path)
        except Exception as e:
            logger.debug(f"Failed to write query cache: {e}")
            return

    def _generate_cache_key(self, data_source: str, sql: str, info) -> str:
        key_parts = [
            data_source,
            sql,
            info.host.get_secret_value(),
            info.port.get_secret_value(),
            info.user.get_secret_value(),
        ]
        key_string = "|".join(key_parts)

        return hashlib.sha256(key_string.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> str:
        return f"/tmp/wren-engine/{cache_key}.cache"
