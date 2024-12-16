import importlib

import httpx
import sqlglot
from anyio import to_thread
from loguru import logger

from app.config import get_config
from app.mdl.core import (
    get_manifest_extractor,
    get_session_context,
    to_json_base64,
)
from app.mdl.java_engine import JavaEngineConnector
from app.model import InternalServerError, UnprocessableEntityError
from app.model.data_source import DataSource

# To register custom dialects from ibis library for sqlglot
importlib.import_module("ibis.backends.sql.dialects")

# Register custom dialects
importlib.import_module("app.custom_sqlglot.dialects")


class Rewriter:
    def __init__(
        self,
        manifest_str: str,
        data_source: DataSource = None,
        java_engine_connector=None,
        experiment=False,
    ):
        self.manifest_str = manifest_str
        self.data_source = data_source
        self.experiment = experiment
        if experiment:
            config = get_config()
            function_path = config.get_remote_function_list_path(data_source)
            self._rewriter = EmbeddedEngineRewriter(manifest_str, function_path)
        else:
            self._rewriter = ExternalEngineRewriter(manifest_str, java_engine_connector)

    async def rewrite(self, sql: str) -> str:
        planned_sql = await self._rewriter.rewrite(sql)
        logger.debug("Planned SQL: {}", planned_sql)
        dialect_sql = self._transpile(planned_sql) if self.data_source else planned_sql
        logger.debug("Dialect SQL: {}", dialect_sql)
        return dialect_sql

    def _transpile(self, planned_sql: str) -> str:
        write = self._get_write_dialect(self.data_source)
        if self.experiment:
            read = None
        else:
            read = "trino"
        return sqlglot.transpile(planned_sql, read=read, write=write)[0]

    @classmethod
    def _get_write_dialect(cls, data_source: DataSource) -> str:
        if data_source == DataSource.canner:
            return "trino"
        return data_source.name


class ExternalEngineRewriter:
    def __init__(self, manifest_str: str, java_engine_connector: JavaEngineConnector):
        self.manifest_str = manifest_str
        self.java_engine_connector = java_engine_connector

    async def rewrite(self, sql: str) -> str:
        try:
            extractor = get_manifest_extractor(self.manifest_str)
            tables = extractor.resolve_used_table_names(sql)
            manifest = extractor.extract_by(tables)
            manifest_str = to_json_base64(manifest)
            return await self.java_engine_connector.dry_plan(manifest_str, sql)
        except httpx.ConnectError as e:
            raise WrenEngineError(f"Can not connect to Java Engine: {e}")
        except httpx.TimeoutException as e:
            raise WrenEngineError(f"Timeout when connecting to Java Engine: {e}")
        except httpx.HTTPStatusError as e:
            raise RewriteError(e.response.text)


class EmbeddedEngineRewriter:
    def __init__(self, manifest_str: str, function_path: str):
        self.manifest_str = manifest_str
        self.function_path = function_path

    async def rewrite(self, sql: str) -> str:
        try:
            extractor = get_manifest_extractor(self.manifest_str)
            tables = extractor.resolve_used_table_names(sql)
            manifest = extractor.extract_by(tables)
            manifest_str = to_json_base64(manifest)
            session_context = get_session_context(manifest_str, self.function_path)
            return await to_thread.run_sync(session_context.transform_sql, sql)
        except Exception as e:
            raise RewriteError(str(e))


class RewriteError(UnprocessableEntityError):
    def __init__(self, message: str):
        super().__init__(message)


class WrenEngineError(InternalServerError):
    def __init__(self, message: str):
        super().__init__(message)
