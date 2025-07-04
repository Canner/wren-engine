import base64
import importlib
import os
from contextlib import closing
from functools import cache
from json import loads
from typing import Any

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import opendal
import pandas as pd
import pyarrow as pa
import sqlglot.expressions as sge
from duckdb import HTTPException, IOException
from google.cloud import bigquery
from google.oauth2 import service_account
from ibis import BaseBackend
from ibis.backends.sql.compilers.postgres import compiler as postgres_compiler
from opentelemetry import trace

from app.model import (
    ConnectionInfo,
    GcsFileConnectionInfo,
    MinioFileConnectionInfo,
    RedshiftConnectionInfo,
    RedshiftConnectionUnion,
    RedshiftIAMConnectionInfo,
    S3FileConnectionInfo,
    UnknownIbisError,
    UnprocessableEntityError,
)
from app.model.data_source import DataSource
from app.model.utils import init_duckdb_gcs, init_duckdb_minio, init_duckdb_s3
from app.util import round_decimal_columns

# Override datatypes of ibis
importlib.import_module("app.custom_ibis.backends.sql.datatypes")

tracer = trace.get_tracer(__name__)


class Connector:
    @tracer.start_as_current_span("connector_init", kind=trace.SpanKind.INTERNAL)
    def __init__(self, data_source: DataSource, connection_info: ConnectionInfo):
        if data_source == DataSource.mssql:
            self._connector = MSSqlConnector(connection_info)
        elif data_source == DataSource.canner:
            self._connector = CannerConnector(connection_info)
        elif data_source == DataSource.bigquery:
            self._connector = BigQueryConnector(connection_info)
        elif data_source in {
            DataSource.local_file,
            DataSource.s3_file,
            DataSource.minio_file,
            DataSource.gcs_file,
        }:
            self._connector = DuckDBConnector(connection_info)
        elif data_source == DataSource.redshift:
            self._connector = RedshiftConnector(connection_info)
        else:
            self._connector = SimpleConnector(data_source, connection_info)

    def query(self, sql: str, limit: int) -> pa.Table:
        return self._connector.query(sql, limit)

    def dry_run(self, sql: str) -> None:
        try:
            self._connector.dry_run(sql)
        except Exception as e:
            raise QueryDryRunError(f"Exception: {type(e)}, message: {e!s}")


class SimpleConnector:
    def __init__(self, data_source: DataSource, connection_info: ConnectionInfo):
        self.data_source = data_source
        self.connection = self.data_source.get_connection(connection_info)

    @tracer.start_as_current_span("connector_query", kind=trace.SpanKind.CLIENT)
    def query(self, sql: str, limit: int) -> pa.Table:
        ibis_table = self.connection.sql(sql).limit(limit)
        ibis_table = round_decimal_columns(ibis_table)
        return ibis_table.to_pyarrow()

    @tracer.start_as_current_span("connector_dry_run", kind=trace.SpanKind.CLIENT)
    def dry_run(self, sql: str) -> None:
        self.connection.sql(sql)


class MSSqlConnector(SimpleConnector):
    def __init__(self, connection_info: ConnectionInfo):
        super().__init__(DataSource.mssql, connection_info)

    def dry_run(self, sql: str) -> None:
        try:
            super().dry_run(sql)
        except AttributeError as e:
            # Workaround for ibis issue #10331
            if e.args[0] == "'NoneType' object has no attribute 'lower'":
                error_message = self._describe_sql_for_error_message(sql)
                raise QueryDryRunError(f"The sql dry run failed. {error_message}.")
            raise UnknownIbisError(e)

    @tracer.start_as_current_span(
        "describe_sql_for_error_message", kind=trace.SpanKind.CLIENT
    )
    def _describe_sql_for_error_message(self, sql: str) -> str:
        tsql = sge.convert(sql).sql("mssql")
        describe_sql = f"SELECT error_message FROM sys.dm_exec_describe_first_result_set({tsql}, NULL, 0)"
        with closing(self.connection.raw_sql(describe_sql)) as cur:
            rows = cur.fetchall()
            if rows is None or len(rows) == 0:
                return "Unknown reason"
            return rows[0][0]


class CannerConnector:
    def __init__(self, connection_info: ConnectionInfo):
        self.connection = DataSource.canner.get_connection(connection_info)

    @tracer.start_as_current_span("connector_query", kind=trace.SpanKind.CLIENT)
    def query(self, sql: str, limit: int) -> pa.Table:
        # Canner enterprise does not support `CREATE TEMPORARY VIEW` for getting schema
        schema = self._get_schema(sql)
        ibis_table = self.connection.sql(sql, schema=schema).limit(limit)
        ibis_table = round_decimal_columns(ibis_table)
        return ibis_table.to_pyarrow()

    @tracer.start_as_current_span("connector_dry_run", kind=trace.SpanKind.CLIENT)
    def dry_run(self, sql: str) -> Any:
        # Canner enterprise does not support dry-run, so we have to query with limit zero
        return self.connection.raw_sql(f"SELECT * FROM ({sql}) LIMIT 0")

    @tracer.start_as_current_span("get_schema", kind=trace.SpanKind.CLIENT)
    def _get_schema(self, sql: str) -> sch.Schema:
        cur = self.dry_run(sql)
        type_names = _get_pg_type_names(self.connection)
        return ibis.schema(
            {
                desc.name: self._to_ibis_type(type_names[desc.type_code])
                for desc in cur.description
            }
        )

    @staticmethod
    def _to_ibis_type(type_name: str) -> dt.DataType:
        return postgres_compiler.type_mapper.from_string(type_name)


class BigQueryConnector(SimpleConnector):
    def __init__(self, connection_info: ConnectionInfo):
        super().__init__(DataSource.bigquery, connection_info)
        self.connection_info = connection_info

    def query(self, sql: str, limit: int) -> pa.Table:
        try:
            return super().query(sql, limit)
        except ValueError as e:
            # Import here to avoid override the custom datatypes
            import ibis.backends.bigquery  # noqa: PLC0415

            # Try to match the error message from the google cloud bigquery library matching Arrow type error.
            # If the error message matches, requries to get the schema from the result and generate a empty pandas dataframe with the mapped schema
            #
            # It's a workaround for the issue that the ibis library does not support empty result for some special types (e.g. JSON or Interval)
            # see details:
            # - https://github.com/Canner/wren-engine/issues/909
            # - https://github.com/ibis-project/ibis/issues/10612
            if "Must pass schema" in str(e):
                with tracer.start_as_current_span(
                    "get_schema", kind=trace.SpanKind.CLIENT
                ):
                    credits_json = loads(
                        base64.b64decode(
                            self.connection_info.credentials.get_secret_value()
                        ).decode("utf-8")
                    )
                    credentials = service_account.Credentials.from_service_account_info(
                        credits_json
                    )
                    credentials = credentials.with_scopes(
                        [
                            "https://www.googleapis.com/auth/drive",
                            "https://www.googleapis.com/auth/cloud-platform",
                        ]
                    )
                    client = bigquery.Client(credentials=credentials)
                    ibis_schema_mapper = ibis.backends.bigquery.BigQuerySchema()
                    bq_fields = client.query(sql).result()
                    ibis_fields = ibis_schema_mapper.to_ibis(bq_fields.schema)
                    return pd.DataFrame(columns=ibis_fields.names)
            else:
                raise e


class DuckDBConnector:
    def __init__(self, connection_info: ConnectionInfo):
        import duckdb  # noqa: PLC0415

        self.connection = duckdb.connect()
        if isinstance(connection_info, S3FileConnectionInfo):
            init_duckdb_s3(self.connection, connection_info)
        if isinstance(connection_info, MinioFileConnectionInfo):
            init_duckdb_minio(self.connection, connection_info)
        if isinstance(connection_info, GcsFileConnectionInfo):
            init_duckdb_gcs(self.connection, connection_info)

        if connection_info.format == "duckdb":
            # For duckdb format, we attach the database files
            self._attach_database(connection_info)

    @tracer.start_as_current_span("duckdb_query", kind=trace.SpanKind.INTERNAL)
    def query(self, sql: str, limit: int | None) -> pa.Table:
        try:
            if limit is None:
                # If no limit is specified, we return the full result
                return self.connection.execute(sql).fetch_arrow_table()
            else:
                # If a limit is specified, we slice the result
                # DuckDB does not support LIMIT in fetch_arrow_table, so we use slice
                # to limit the number of rows returned
                return (
                    self.connection.execute(sql).fetch_arrow_table().slice(length=limit)
                )
        except IOException as e:
            raise UnprocessableEntityError(f"Failed to execute query: {e!s}")
        except HTTPException as e:
            raise UnprocessableEntityError(f"Failed to execute query: {e!s}")

    @tracer.start_as_current_span("duckdb_dry_run", kind=trace.SpanKind.INTERNAL)
    def dry_run(self, sql: str) -> None:
        try:
            self.connection.execute(sql)
        except IOException as e:
            raise QueryDryRunError(f"Failed to execute query: {e!s}")
        except HTTPException as e:
            raise QueryDryRunError(f"Failed to execute query: {e!s}")

    def _attach_database(self, connection_info: ConnectionInfo) -> None:
        db_files = self._list_duckdb_files(connection_info)
        if not db_files:
            raise UnprocessableEntityError(
                "No DuckDB files found in the specified path."
            )

        for file in db_files:
            try:
                self.connection.execute(
                    f"ATTACH DATABASE '{file}' AS \"{os.path.splitext(os.path.basename(file))[0]}\" (READ_ONLY);"
                )
            except IOException as e:
                raise UnprocessableEntityError(f"Failed to attach database: {e!s}")
            except HTTPException as e:
                raise UnprocessableEntityError(f"Failed to attach database: {e!s}")

    def _list_duckdb_files(self, connection_info: ConnectionInfo) -> list[str]:
        # This method should return a list of file paths in the DuckDB database
        op = opendal.Operator("fs", root=connection_info.url.get_secret_value())
        files = []
        try:
            for file in op.list("/"):
                if file.path != "/":
                    stat = op.stat(file.path)
                    if not stat.mode.is_dir() and file.path.endswith(".duckdb"):
                        full_path = (
                            f"{connection_info.url.get_secret_value()}/{file.path}"
                        )
                        files.append(full_path)
        except Exception as e:
            raise UnprocessableEntityError(f"Failed to list files: {e!s}")

        return files


class RedshiftConnector:
    def __init__(self, connection_info: RedshiftConnectionUnion):
        import redshift_connector  # noqa: PLC0415

        if isinstance(connection_info, RedshiftIAMConnectionInfo):
            self.connection = redshift_connector.connect(
                iam=True,
                cluster_identifier=connection_info.cluster_identifier.get_secret_value(),
                database=connection_info.database.get_secret_value(),
                db_user=connection_info.user.get_secret_value(),
                access_key_id=connection_info.access_key_id.get_secret_value(),
                secret_access_key=connection_info.access_key_secret.get_secret_value(),
                region=connection_info.region.get_secret_value(),
            )
        elif isinstance(connection_info, RedshiftConnectionInfo):
            self.connection = redshift_connector.connect(
                host=connection_info.host.get_secret_value(),
                port=connection_info.port.get_secret_value(),
                database=connection_info.database.get_secret_value(),
                user=connection_info.user.get_secret_value(),
                password=connection_info.password.get_secret_value(),
            )
        else:
            raise ValueError("Invalid Redshift connection_info type")

    @tracer.start_as_current_span("connector_query", kind=trace.SpanKind.CLIENT)
    def query(self, sql: str, limit: int) -> pa.Table:
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=cols).head(limit)
            return pa.Table.from_pandas(df)

    @tracer.start_as_current_span("connector_dry_run", kind=trace.SpanKind.CLIENT)
    def dry_run(self, sql: str) -> None:
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(f"SELECT * FROM ({sql}) AS sub LIMIT 0")


@cache
def _get_pg_type_names(connection: BaseBackend) -> dict[int, str]:
    with closing(connection.raw_sql("SELECT oid, typname FROM pg_type")) as cur:
        return dict(cur.fetchall())


class QueryDryRunError(UnprocessableEntityError):
    pass
