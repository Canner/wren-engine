import hashlib
import time
from typing import Any, Optional


class QueryCacheManager:
    def __init__(self):
        # e.g. {cache_key: (result, timestamp)}
        self.cache: dict[str, tuple[Any, Any]] = {}

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

    def get(self, data_source: str, sql: str, info) -> Optional[Any]:
        cache_key = self._generate_cache_key(data_source, sql, info)

        if cache_key in self.cache:
            result, _ = self.cache[cache_key]
            return result

        return None

    def set(self, data_source: str, sql: str, result: Any, info) -> None:
        cache_key = self._generate_cache_key(data_source, sql, info)
        self.cache[cache_key] = (result, time.time())
