from abc import ABC, abstractmethod

import httpx
import orjson
import sqlglot
from loguru import logger
from wren_core import transform_sql

from app.config import get_config
from app.model import UnprocessableEntityError
from app.model.data_source import DataSource

wren_engine_endpoint = get_config().wren_engine_endpoint


class Rewriter(ABC):
    def __init__(self, manifest_str: str, data_source: DataSource = None):
        self.manifest_str = manifest_str
        self.data_source = data_source

    @abstractmethod
    def rewrite(self, sql: str) -> str:
        pass

    def transpile(self, planned_sql: str) -> str:
        dialect_sql = sqlglot.transpile(
            planned_sql, read="trino", write=self.data_source.name
        )[0]
        logger.debug("Dialect SQL: {}", dialect_sql)
        return dialect_sql


class ExternalEngineRewriter(Rewriter):
    def __init__(self, manifest_str: str, data_source: DataSource = None):
        super().__init__(manifest_str, data_source)

    def rewrite(self, sql: str) -> str:
        try:
            r = httpx.request(
                method="GET",
                url=f"{wren_engine_endpoint}/v2/mdl/dry-plan",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                content=orjson.dumps({"manifestStr": self.manifest_str, "sql": sql}),
            )
            planned_sql = r.raise_for_status().text
            logger.debug("Planned SQL: {}", planned_sql)
            return (
                planned_sql if self.data_source is None else self.transpile(planned_sql)
            )
        except httpx.ConnectError as e:
            raise ConnectionError(f"Can not connect to Wren Engine: {e}")
        except httpx.HTTPStatusError as e:
            raise RewriteError(e.response.text)


class EmbeddedEngineRewriter(Rewriter):
    def __init__(self, manifest_str: str, data_source: DataSource = None):
        super().__init__(manifest_str, data_source)

    def rewrite(self, sql: str) -> str:
        planned_sql = transform_sql(self.manifest_str, sql)
        logger.debug("Planned SQL: {}", planned_sql)
        return planned_sql if self.data_source is None else self.transpile(planned_sql)


class RewriteError(UnprocessableEntityError):
    def __init__(self, message: str):
        super().__init__(message)
