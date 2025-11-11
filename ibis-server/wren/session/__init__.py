import uuid

import pyarrow as pa
import sqlglot

from app.config import get_config
from app.mdl.core import get_manifest_extractor, to_json_base64
from app.mdl.rewriter import EmbeddedEngineRewriter
from app.model import ConnectionInfo
from app.model.connector import Connector
from app.model.data_source import DataSource
from app.model.error import ErrorCode, WrenError
from app.util import to_json


class Context:
    def __init__(
        self,
        data_source: DataSource | None,
        connection_info: ConnectionInfo | None,
        manifest_base64: str | None,
        context_id: str | None = None,
    ):
        self.data_source = data_source
        self.connection_info = connection_info
        self.manifest_base64 = manifest_base64
        self.context_id = context_id or str(uuid.uuid4())
        self.user_id = None
        self.rewriter = EmbeddedEngineRewriter(
            get_config().get_remote_function_list_path(data_source)
        )
        self._connector = None

    def __repr__(self):
        return f"Context(id={self.context_id}, data_source={self.data_source})"

    def sql(self, sql, properties: dict | None = None):
        """Create a Task with the given SQL and plan it based on the MDL.

        Parameters
        ----------
        sql : str
            The SQL statement to be executed.
        properties : dict, optional
            Additional properties to be used in the task.

        Returns
        -------
        Task
            A Task object that contains the planned SQL and other properties.
        """
        return Task(context=self, properties=properties).plan(sql)

    def get_connector(self):
        """Get the connector for the context's data source.

        Returns
        -------
        Connector
            The connector for the context's data source.
        """
        if self._connector is None:
            self._connector = Connector(self.data_source, self.connection_info)
        return self._connector


class Task:
    def __init__(
        self,
        context: Context | None,
        properties: dict | None,
        task_id: str | None = None,
    ):
        self.task_id: str = task_id or str(uuid.uuid4())
        self.context: Context = context
        self.properties: dict = self._lowrcase_properties(properties)
        self.wren_sql: str | None = None
        self.planned_sql: str | None = None
        self.dialect_sql: str | None = None
        self.results: pa.Table | None = None
        self.manifest: str | None = None

    def _lowrcase_properties(self, properties: dict | None) -> dict:
        """Convert all keys in the properties dictionary to lowercase."""
        if properties is None:
            return {}
        return {k.lower(): v for k, v in properties.items()}

    def __repr__(self):
        return f"Task(id={self.task_id}, context={self.context})"

    def plan(self, input_sql):
        """Plan input Wren SQL based on the MDL to the planned SQL and transpiled dialect SQL."""
        self.wren_sql = input_sql
        self.manifest = self._extract_manifest(
            self.context.manifest_base64, self.wren_sql
        )

        self.planned_sql = self.context.rewriter.rewrite_sync(
            self.manifest,
            self.wren_sql,
            self.properties,
        )

        read = self._get_read_dialect()
        write = self._get_write_dialect()
        self.dialect_sql = sqlglot.transpile(self.planned_sql, read=read, write=write)[
            0
        ]
        return self

    def _extract_manifest(self, manifest_str: str, sql: str) -> str:
        try:
            extractor = get_manifest_extractor(manifest_str)
            tables = extractor.resolve_used_table_names(sql)
            manifest = extractor.extract_by(tables)
            return to_json_base64(manifest)
        except Exception as e:
            self.context.rewriter.handle_extract_exception(e)

    def _get_read_dialect(self) -> str | None:
        return None

    def _get_write_dialect(
        self,
    ) -> str:
        if self.context.data_source == DataSource.canner:
            return "trino"
        elif self.context.data_source in {
            DataSource.local_file,
            DataSource.s3_file,
            DataSource.minio_file,
            DataSource.gcs_file,
        }:
            return "duckdb"
        return self.context.data_source.name

    def dry_run(self):
        """Perform a dry run of the dialect SQL without executing it."""
        if self.dialect_sql is None:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                "Dialect SQL is not set. Call transpile() first.",
            )
        self.context.get_connector().dry_run(self.dialect_sql)

    def execute(self, limit: int | None = None):
        """Execute the dialect SQL and return the results.

        Parameters
        ----------
        limit : int, optional
            The maximum number of rows to return. If None, returns all rows.
        """
        if self.context.connection_info is None:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                "Connection info is not set. Cannot execute without connection info.",
            )
        if self.dialect_sql is None:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                "Dialect SQL is not set. Call transpile() first.",
            )
        self.results = self.context.get_connector().query(self.dialect_sql, limit)
        return self

    def formatted_result(self):
        """Get the formatted result of the executed task."""
        if self.results is None:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                "Results are not set. Call execute() first.",
            )

        return to_json(self.results, self.properties, self.context.data_source)
