"""WrenEngine — SQL transform + execute against a data source.

Example usage:

    from wren.engine import WrenEngine
    from wren.model.data_source import DataSource

    engine = WrenEngine(
        manifest_str="<base64-encoded MDL JSON>",
        data_source=DataSource.postgres,
        connection_info={"host": "localhost", "port": 5432, ...},
    )

    # Transform only (no DB required)
    planned_sql = engine.transpile("SELECT * FROM orders")

    # Execute against the data source
    arrow_table = engine.query("SELECT * FROM orders", limit=100)
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import sqlglot

from wren.connector.factory import get_connector
from wren.mdl import get_manifest_extractor, get_session_context, to_json_base64
from wren.model.data_source import DataSource
from wren.model.error import DIALECT_SQL, PLANNED_SQL, ErrorCode, ErrorPhase, WrenError


def _get_write_dialect(data_source: DataSource) -> str:
    if data_source == DataSource.canner:
        return "trino"
    if data_source in {
        DataSource.local_file,
        DataSource.s3_file,
        DataSource.minio_file,
        DataSource.gcs_file,
    }:
        return "duckdb"
    return data_source.name


class WrenEngine:
    """Thin facade over wren-core MDL processing and connector execution.

    Parameters
    ----------
    manifest_str:
        Base64-encoded MDL JSON string (as produced by ``wren_core.to_json_base64``).
    data_source:
        Target data source enum value.
    connection_info:
        Dict of connection parameters OR a typed ConnectionInfo object.
    function_path:
        Optional path to a CSV file of custom function definitions.
        Passed through to wren-core SessionContext.
    """

    def __init__(
        self,
        manifest_str: str,
        data_source: DataSource | str,
        connection_info: dict[str, Any] | object,
        function_path: str | None = None,
    ):
        if isinstance(data_source, str):
            data_source = DataSource(data_source)

        self.manifest_str = manifest_str
        self.data_source = data_source
        self.function_path = function_path

        # Build typed ConnectionInfo if a raw dict was given.
        # An empty dict is allowed for transpile-only usage (no DB connection).
        if isinstance(connection_info, dict) and connection_info:
            self.connection_info = data_source.get_connection_info(connection_info)
        else:
            self.connection_info = connection_info

        self._connector = None

    # ------------------------------------------------------------------
    # SQL transformation (no DB access)
    # ------------------------------------------------------------------

    def transpile(self, sql: str, properties: dict | None = None) -> str:
        """Transform SQL through MDL and transpile to the target dialect.

        Returns the dialect SQL string without executing it.
        """
        planned = self._plan(sql, properties)
        return self._transpile(planned)

    def dry_plan(self, sql: str, properties: dict | None = None) -> str:
        """Return the wren-core planned SQL (DataFusion dialect, before transpile)."""
        return self._plan(sql, properties)

    # ------------------------------------------------------------------
    # SQL execution
    # ------------------------------------------------------------------

    def query(
        self,
        sql: str,
        limit: int | None = None,
        properties: dict | None = None,
    ) -> pa.Table:
        """Transpile and execute SQL, return results as an Arrow table."""
        dialect_sql = self.transpile(sql, properties)
        connector = self._get_connector()
        try:
            return connector.query(dialect_sql, limit)
        except WrenError:
            raise
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                str(e),
                phase=ErrorPhase.SQL_EXECUTION,
                metadata={DIALECT_SQL: dialect_sql},
            ) from e

    def dry_run(self, sql: str, properties: dict | None = None) -> None:
        """Transpile and dry-run SQL without returning results."""
        dialect_sql = self.transpile(sql, properties)
        connector = self._get_connector()
        try:
            connector.dry_run(dialect_sql)
        except WrenError:
            raise
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                str(e),
                phase=ErrorPhase.SQL_DRY_RUN,
                metadata={DIALECT_SQL: dialect_sql},
            ) from e

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._connector is not None:
            self._connector.close()
            self._connector = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _plan(self, sql: str, properties: dict | None) -> str:
        processed = None
        if properties:
            processed = frozenset(properties.items())

        try:
            # Extract minimal manifest for the query
            extractor = get_manifest_extractor(self.manifest_str)
            tables = extractor.resolve_used_table_names(sql)
            manifest = extractor.extract_by(tables)
            effective_manifest = to_json_base64(manifest)
        except Exception:
            effective_manifest = self.manifest_str

        try:
            session = get_session_context(
                effective_manifest,
                self.function_path,
                processed,
                self.data_source.name,
            )
            return session.transform_sql(sql)
        except Exception as e:
            raise WrenError(
                ErrorCode.INVALID_SQL,
                str(e),
                phase=ErrorPhase.SQL_PLANNING,
                metadata={DIALECT_SQL: sql},
            ) from e

    def _transpile(self, planned_sql: str) -> str:
        try:
            write = _get_write_dialect(self.data_source)
            return sqlglot.transpile(planned_sql, read="duckdb", write=write)[0]
        except Exception as e:
            raise WrenError(
                ErrorCode.SQLGLOT_ERROR,
                str(e),
                phase=ErrorPhase.SQL_TRANSPILE,
                metadata={PLANNED_SQL: planned_sql},
            ) from e

    def _get_connector(self):
        if self._connector is None:
            self._connector = get_connector(self.data_source, self.connection_info)
        return self._connector
