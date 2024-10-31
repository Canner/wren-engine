import importlib

import httpx
import orjson
import sqlglot
from loguru import logger

from app.config import get_config
from app.mdl.context import get_session_context
from app.model import InternalServerError, UnprocessableEntityError
from app.model.data_source import DataSource

wren_engine_endpoint = get_config().wren_engine_endpoint

# To register custom dialects from ibis library for sqlglot
importlib.import_module("ibis.backends.sql.dialects")

# Register custom dialects
importlib.import_module("app.custom_sqlglot.dialects")


class Rewriter:
    def __init__(
        self,
        manifest_str: str,
        data_source: DataSource = None,
        experiment=False,
    ):
        self.manifest_str = manifest_str
        self.data_source = data_source
        if experiment:
            config = get_config()
            function_path = config.get_remote_function_list_path(data_source)
            self._rewriter = EmbeddedEngineRewriter(manifest_str, function_path)
        else:
            self._rewriter = ExternalEngineRewriter(manifest_str)

    def rewrite(self, sql: str) -> str:
        planned_sql = self._rewriter.rewrite(sql)
        logger.debug("Planned SQL: {}", planned_sql)
        dialect_sql = self._transpile(planned_sql) if self.data_source else planned_sql
        logger.debug("Dialect SQL: {}", dialect_sql)
        return dialect_sql

    def _transpile(self, planned_sql: str) -> str:
        write = self._get_write_dialect(self.data_source)
        return sqlglot.transpile(planned_sql, read="trino", write=write)[0]

    @classmethod
    def _get_write_dialect(cls, data_source: DataSource) -> str:
        if data_source == DataSource.canner:
            return "trino"
        return data_source.name


class ExternalEngineRewriter:
    def __init__(self, manifest_str: str):
        self.manifest_str = manifest_str

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
            return r.raise_for_status().text.replace("\n", " ")
        except httpx.ConnectError as e:
            raise WrenEngineError(f"Can not connect to Wren Engine: {e}")
        except httpx.TimeoutException as e:
            raise WrenEngineError(f"Timeout when connecting to Wren Engine: {e}")
        except httpx.HTTPStatusError as e:
            raise RewriteError(e.response.text)


class EmbeddedEngineRewriter:
    def __init__(self, manifest_str: str, function_path: str):
        self.manifest_str = manifest_str
        self.function_path = function_path

    def rewrite(self, sql: str) -> str:
        try:
            session_context = get_session_context(self.manifest_str, self.function_path)
            return session_context.transform_sql(sql)
        except Exception as e:
            raise RewriteError(str(e))


class RewriteError(UnprocessableEntityError):
    def __init__(self, message: str):
        super().__init__(message)


class WrenEngineError(InternalServerError):
    def __init__(self, message: str):
        super().__init__(message)
