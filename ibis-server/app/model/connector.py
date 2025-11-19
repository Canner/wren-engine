import base64
import importlib
import os
import time
from contextlib import closing, suppress
from decimal import Decimal as PyDecimal
from functools import cache
from json import loads
from typing import Any

try:
    import clickhouse_connect

    ClickHouseDbError = clickhouse_connect.driver.exceptions.DatabaseError
except ImportError:  # pragma: no cover

    class ClickHouseDbError(Exception):
        pass


import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import opendal
import pandas as pd
import psycopg
import pyarrow as pa
import sqlglot.expressions as sge
import trino
from databricks import sql as dbsql
from databricks.sdk.core import Config as DbConfig
from databricks.sdk.core import oauth_service_principal
from duckdb import HTTPException, IOException
from google.cloud import bigquery
from google.oauth2 import service_account
from ibis import BaseBackend
from ibis.backends.sql.compilers.postgres import compiler as postgres_compiler
from ibis.expr.datatypes import Decimal
from ibis.expr.datatypes.core import UUID
from ibis.expr.types import Table
from loguru import logger
from opentelemetry import trace

from app.model import (
    ConnectionInfo,
    DatabricksConnectionUnion,
    DatabricksServicePrincipalConnectionInfo,
    DatabricksTokenConnectionInfo,
    GcsFileConnectionInfo,
    MinioFileConnectionInfo,
    RedshiftConnectionInfo,
    RedshiftConnectionUnion,
    RedshiftIAMConnectionInfo,
    S3FileConnectionInfo,
)
from app.model.data_source import DataSource
from app.model.error import (
    DIALECT_SQL,
    ErrorCode,
    ErrorPhase,
    WrenError,
)
from app.model.utils import init_duckdb_gcs, init_duckdb_minio, init_duckdb_s3

# Override datatypes of ibis
importlib.import_module("app.custom_ibis.backends.sql.datatypes")

tracer = trace.get_tracer(__name__)


@cache
def _get_pg_type_names(connection: BaseBackend) -> dict[int, str]:
    with closing(connection.raw_sql("SELECT oid, typname FROM pg_type")) as cur:
        return dict(cur.fetchall())


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
        elif data_source == DataSource.postgres:
            self._connector = PostgresConnector(connection_info)
        elif data_source == DataSource.databricks:
            self._connector = DatabricksConnector(connection_info)
        else:
            self._connector = SimpleConnector(data_source, connection_info)

    def query(self, sql: str, limit: int | None = None) -> pa.Table:
        try:
            return self._connector.query(sql, limit)
        except (
            WrenError,
            TimeoutError,
            psycopg.errors.QueryCanceled,
        ):
            raise
        except trino.exceptions.TrinoQueryError as e:
            if not e.error_name == "EXCEEDED_TIME_LIMIT":
                raise WrenError(
                    ErrorCode.INVALID_SQL,
                    str(e),
                    phase=ErrorPhase.SQL_DRY_RUN,
                    metadata={DIALECT_SQL: sql},
                ) from e
            raise
        except ClickHouseDbError as e:
            if "TIMEOUT_EXCEEDED" not in str(e):
                raise WrenError(
                    ErrorCode.INVALID_SQL,
                    str(e),
                    phase=ErrorPhase.SQL_EXECUTION,
                    metadata={DIALECT_SQL: sql},
                ) from e
            raise e
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                str(e),
                phase=ErrorPhase.SQL_EXECUTION,
                metadata={DIALECT_SQL: sql},
            ) from e

    def dry_run(self, sql: str) -> None:
        try:
            self._connector.dry_run(sql)
        except (
            WrenError,
            TimeoutError,
            psycopg.errors.QueryCanceled,
        ):
            raise
        except trino.exceptions.TrinoQueryError as e:
            if not e.error_name == "EXCEEDED_TIME_LIMIT":
                raise WrenError(
                    ErrorCode.INVALID_SQL,
                    str(e),
                    phase=ErrorPhase.SQL_DRY_RUN,
                    metadata={DIALECT_SQL: sql},
                ) from e
            raise
        except ClickHouseDbError as e:
            if "TIMEOUT_EXCEEDED" not in str(e):
                raise WrenError(
                    ErrorCode.INVALID_SQL,
                    str(e),
                    phase=ErrorPhase.SQL_DRY_RUN,
                    metadata={DIALECT_SQL: sql},
                ) from e
            raise
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                str(e),
                phase=ErrorPhase.SQL_DRY_RUN,
                metadata={DIALECT_SQL: sql},
            ) from e

    def close(self) -> None:
        """Close the underlying connection."""
        if hasattr(self._connector, "close"):
            self._connector.close()
        else:
            logger.warning(
                f"Close method not implemented for {type(self._connector).__name__}"
            )


class SimpleConnector:
    def __init__(self, data_source: DataSource, connection_info: ConnectionInfo):
        self.data_source = data_source
        self.connection = self.data_source.get_connection(connection_info)
        self._closed = False

    @tracer.start_as_current_span("connector_query", kind=trace.SpanKind.CLIENT)
    def query(self, sql: str, limit: int | None = None) -> pa.Table:
        ibis_table = self.connection.sql(sql)
        if limit is not None:
            ibis_table = ibis_table.limit(limit)
        ibis_table = self._handle_pyarrow_unsupported_type(ibis_table)
        return ibis_table.to_pyarrow()

    def _handle_pyarrow_unsupported_type(self, ibis_table: Table, **kwargs) -> Table:
        result_table = ibis_table
        for name, dtype in ibis_table.schema().items():
            if isinstance(dtype, Decimal):
                # Round decimal columns to a specified scale
                result_table = self._round_decimal_columns(
                    result_table=result_table, col_name=name, **kwargs
                )
            elif isinstance(dtype, UUID):
                # Convert UUID to string for compatibility
                result_table = self._cast_uuid_columns(
                    result_table=result_table, col_name=name
                )

        return result_table

    def _cast_uuid_columns(self, result_table: Table, col_name: str) -> Table:
        col = result_table[col_name]
        # Convert UUID to string for compatibility
        casted_col = col.cast("string")
        return result_table.mutate(**{col_name: casted_col})

    def _round_decimal_columns(
        self, result_table: Table, col_name, scale: int = 9
    ) -> Table:
        col = result_table[col_name]
        # Maximum precision for pyarrow decimal is 38
        decimal_type = Decimal(precision=38, scale=scale)
        rounded_col = col.cast(decimal_type).round(scale)
        return result_table.mutate(**{col_name: rounded_col})

    @tracer.start_as_current_span("connector_dry_run", kind=trace.SpanKind.CLIENT)
    def dry_run(self, sql: str) -> None:
        self.connection.sql(sql)

    def close(self) -> None:
        """Close the connection safely."""
        if self._closed or not hasattr(self, "connection") or self.connection is None:
            return

        try:
            if hasattr(self.connection, "con"):
                # If the connection has a 'con' attribute (default for many ibis backends), try to invoke close()
                if hasattr(self.connection.con, "close"):
                    self.connection.con.close()
            elif hasattr(self.connection, "close"):
                # Try to close the connection directly if it has a close method
                self.connection.close()
            elif hasattr(self.connection, "disconnect"):
                # Some backends use disconnect instead of close
                self.connection.disconnect()
            else:
                logger.warning(
                    f"Closing connection for {self.data_source.value} is not implemented."
                )
        except Exception as e:
            logger.warning(
                f"Error closing connection for {self.data_source.value}: {e}"
            )
        finally:
            # Mark connection as closed to prevent double-close
            self._closed = True
            self.connection = None


class PostgresConnector(SimpleConnector):
    def __init__(self, connection_info):
        super().__init__(DataSource.postgres, connection_info)

    def close(self) -> None:
        """Safely close postgres connection to prevent segfault."""
        if self._closed or not hasattr(self, "connection") or self.connection is None:
            return

        try:
            # Check if the underlying psycopg2 connection exists and is not already closed
            if hasattr(self.connection, "con") and self.connection.con is not None:
                # Check if connection is still alive before closing
                if (
                    hasattr(self.connection.con, "closed")
                    and not self.connection.con.closed
                ):
                    # Cancel any running queries first
                    with suppress(Exception):
                        self.connection.con.cancel()

                    time.sleep(0.1)

                    # Close the connection
                    self.connection.con.close()
                elif (
                    hasattr(self.connection.con, "closed")
                    and self.connection.con.closed
                ):
                    # Connection is already closed, just log
                    logger.debug("Postgres connection already closed")
            elif hasattr(self.connection, "close"):
                self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing postgres connection: {e}")
            # Force close by setting connection to None
            if hasattr(self.connection, "con"):
                self.connection.con = None
        finally:
            # Mark connection as closed to prevent double-close
            self._closed = True
            self.connection = None


class MSSqlConnector(SimpleConnector):
    def __init__(self, connection_info: ConnectionInfo):
        super().__init__(DataSource.mssql, connection_info)

    @tracer.start_as_current_span("connector_query", kind=trace.SpanKind.CLIENT)
    def query(self, sql: str, limit: int | None = None) -> pa.Table:
        ibis_table = self.connection.sql(sql)
        if limit is not None:
            ibis_table = ibis_table.limit(limit)
        return self._round_decimal_columns(ibis_table)

    def _round_decimal_columns(self, ibis_table: Table, scale: int = 9) -> pa.Table:
        def round_decimal(val):
            if val is None:
                return None
            d = PyDecimal(str(val))
            quant = PyDecimal("1." + "0" * scale)
            return d.quantize(quant)

        decimal_columns = []
        for name, dtype in ibis_table.schema().items():
            if isinstance(dtype, Decimal):
                decimal_columns.append(name)

        # If no decimal columns, return original table unchanged
        if not decimal_columns:
            return ibis_table.to_pyarrow()

        pandas_df = ibis_table.to_pandas()
        for col_name in decimal_columns:
            pandas_df[col_name] = pandas_df[col_name].apply(round_decimal)

        arrow_table = pa.Table.from_pandas(pandas_df)
        return arrow_table

    def dry_run(self, sql: str) -> None:
        try:
            super().dry_run(sql)
        except AttributeError as e:
            # Workaround for ibis issue #10331
            if e.args[0] == "'NoneType' object has no attribute 'lower'":
                error_message = self._describe_sql_for_error_message(sql)
                raise WrenError(
                    error_code=ErrorCode.INVALID_SQL,
                    message=f"The sql dry run failed. {error_message}.",
                    phase=ErrorPhase.SQL_DRY_RUN,
                    metadata={DIALECT_SQL: sql},
                ) from e
            raise WrenError(
                error_code=ErrorCode.IBIS_PROJECT_ERROR,
                message=str(e),
                phase=ErrorPhase.SQL_DRY_RUN,
            ) from e

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
    def query(self, sql: str, limit: int | None = None) -> pa.Table:
        # Canner enterprise does not support `CREATE TEMPORARY VIEW` for getting schema
        schema = self._get_schema(sql)
        ibis_table = self.connection.sql(sql, schema=schema)
        if limit is not None:
            ibis_table = ibis_table.limit(limit)
        ibis_table = self._handle_pyarrow_unsupported_type(ibis_table)
        return ibis_table.to_pyarrow()

    def _handle_pyarrow_unsupported_type(self, ibis_table: Table, **kwargs) -> Table:
        result_table = ibis_table
        for name, dtype in ibis_table.schema().items():
            if isinstance(dtype, Decimal):
                # Round decimal columns to a specified scale
                result_table = self._round_decimal_columns(
                    result_table=result_table, col_name=name, **kwargs
                )
            elif isinstance(dtype, UUID):
                # Convert UUID to string for compatibility
                result_table = self._cast_uuid_columns(
                    result_table=result_table, col_name=name
                )

        return result_table

    @tracer.start_as_current_span("connector_dry_run", kind=trace.SpanKind.CLIENT)
    def dry_run(self, sql: str) -> Any:
        # Canner enterprise does not support dry-run, so we have to query with limit zero
        return self.connection.raw_sql(f"SELECT * FROM ({sql}) LIMIT 0")

    def close(self) -> None:
        """Close the Canner connection."""
        try:
            if hasattr(self.connection, "con") and hasattr(
                self.connection.con, "close"
            ):
                self.connection.con.close()
            elif hasattr(self.connection, "close"):
                self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing Canner connection: {e}")

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

    @tracer.start_as_current_span("connector_query", kind=trace.SpanKind.CLIENT)
    def query(self, sql: str, limit: int | None = None) -> pa.Table:
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
        return client.query(sql).result(max_results=limit).to_arrow()


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
        if limit is None:
            # If no limit is specified, we return the full result
            return self.connection.execute(sql).fetch_arrow_table()
        else:
            # If a limit is specified, we slice the result
            # DuckDB does not support LIMIT in fetch_arrow_table, so we use slice
            # to limit the number of rows returned
            return self.connection.execute(sql).fetch_arrow_table().slice(length=limit)

    @tracer.start_as_current_span("duckdb_dry_run", kind=trace.SpanKind.INTERNAL)
    def dry_run(self, sql: str) -> None:
        self.connection.execute(sql)

    def _attach_database(self, connection_info: ConnectionInfo) -> None:
        db_files = self._list_duckdb_files(connection_info)
        if not db_files:
            raise WrenError(
                ErrorCode.DUCKDB_FILE_NOT_FOUND,
                "No DuckDB files found in the specified path.",
            )

        for file in db_files:
            try:
                self.connection.execute(
                    f"ATTACH DATABASE '{file}' AS \"{os.path.splitext(os.path.basename(file))[0]}\" (READ_ONLY);"
                )
            except IOException as e:
                raise WrenError(
                    ErrorCode.ATTACH_DUCKDB_ERROR, f"Failed to attach database: {e!s}"
                )
            except HTTPException as e:
                raise WrenError(
                    ErrorCode.ATTACH_DUCKDB_ERROR, f"Failed to attach database: {e!s}"
                )

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
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR, f"Failed to list files: {e!s}"
            )

        return files

    def close(self) -> None:
        """Close the DuckDB connection."""
        try:
            self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing DuckDB connection: {e}")


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
            raise WrenError(
                ErrorCode.GENERIC_INTERNAL_ERROR,
                "Invalid Redshift connection_info type",
            )

        # Enable autocommit to prevent holding AccessShareLock indefinitely
        # This ensures locks are released immediately after query execution
        self.connection.autocommit = True

    @tracer.start_as_current_span("connector_query", kind=trace.SpanKind.CLIENT)
    def query(self, sql: str, limit: int | None = None) -> pa.Table:
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=cols)
            if limit is not None:
                df = df.head(limit)
            return pa.Table.from_pandas(df)

    @tracer.start_as_current_span("connector_dry_run", kind=trace.SpanKind.CLIENT)
    def dry_run(self, sql: str) -> None:
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(f"SELECT * FROM ({sql}) AS sub LIMIT 0")

    def close(self) -> None:
        """Close the Redshift connection."""
        try:
            self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing Redshift connection: {e}")


class DatabricksConnector(SimpleConnector):
    def __init__(self, connection_info: DatabricksConnectionUnion):
        if isinstance(connection_info, DatabricksTokenConnectionInfo):
            self.connection = dbsql.connect(
                server_hostname=connection_info.server_hostname.get_secret_value(),
                http_path=connection_info.http_path.get_secret_value(),
                access_token=connection_info.access_token.get_secret_value(),
            )
        elif isinstance(connection_info, DatabricksServicePrincipalConnectionInfo):
            kwargs = {
                "host": connection_info.server_hostname.get_secret_value(),
                "client_id": connection_info.client_id.get_secret_value(),
                "client_secret": connection_info.client_secret.get_secret_value(),
            }
            if connection_info.azure_tenant_id is not None:
                kwargs["azure_tenant_id"] = (
                    connection_info.azure_tenant_id.get_secret_value()
                )

            def credential_provider():
                return oauth_service_principal(DbConfig(**kwargs))

            self.connection = dbsql.connect(
                server_hostname=connection_info.server_hostname.get_secret_value(),
                http_path=connection_info.http_path.get_secret_value(),
                credentials_provider=credential_provider,
            )

    def query(self, sql, limit=None):
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql)

            if limit is not None:
                arrow_table = cursor.fetchmany_arrow(limit)
            else:
                arrow_table = cursor.fetchall_arrow()

            return arrow_table

    def dry_run(self, sql):
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(f"SELECT * FROM ({sql}) AS sub LIMIT 0")

    def close(self) -> None:
        """Close the Databricks connection."""
        try:
            self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing Databricks connection: {e}")
