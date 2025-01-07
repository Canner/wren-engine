import base64
import importlib
from functools import cache
from json import loads
from typing import Any

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import ibis.formats
import pandas as pd
import sqlglot.expressions as sge
from google.cloud import bigquery
from google.oauth2 import service_account
from ibis import BaseBackend
from ibis.backends.sql.compilers.postgres import compiler as postgres_compiler

from app.model import ConnectionInfo, UnknownIbisError, UnprocessableEntityError
from app.model.data_source import DataSource

# Override datatypes of ibis
importlib.import_module("app.custom_ibis.backends.sql.datatypes")


class Connector:
    def __init__(self, data_source: DataSource, connection_info: ConnectionInfo):
        if data_source == DataSource.mssql:
            self._connector = MSSqlConnector(connection_info)
        elif data_source == DataSource.canner:
            self._connector = CannerConnector(connection_info)
        elif data_source == DataSource.bigquery:
            self._connector = BigQueryConnector(connection_info)
        elif data_source == DataSource.local_file:
            self._connector = DuckDBConnector(connection_info)
        else:
            self._connector = SimpleConnector(data_source, connection_info)

    def query(self, sql: str, limit: int) -> pd.DataFrame:
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

    def query(self, sql: str, limit: int) -> pd.DataFrame:
        return self.connection.sql(sql).limit(limit).to_pandas()

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

    def _describe_sql_for_error_message(self, sql: str) -> str:
        tsql = sge.convert(sql).sql("mssql")
        describe_sql = f"SELECT error_message FROM sys.dm_exec_describe_first_result_set({tsql}, NULL, 0)"
        with self.connection.raw_sql(describe_sql) as cur:
            rows = cur.fetchall()
            if rows is None or len(rows) == 0:
                return "Unknown reason"
            return rows[0][0]


class CannerConnector:
    def __init__(self, connection_info: ConnectionInfo):
        self.connection = DataSource.canner.get_connection(connection_info)

    def query(self, sql: str, limit: int) -> pd.DataFrame:
        # Canner enterprise does not support `CREATE TEMPORARY VIEW` for getting schema
        schema = self._get_schema(sql)
        return self.connection.sql(sql, schema=schema).limit(limit).to_pandas()

    def dry_run(self, sql: str) -> Any:
        # Canner enterprise does not support dry-run, so we have to query with limit zero
        return self.connection.raw_sql(f"SELECT * FROM ({sql}) LIMIT 0")

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

    def query(self, sql: str, limit: int) -> pd.DataFrame:
        try:
            return super().query(sql, limit)
        except ValueError as e:
            # Import here to avoid override the custom datatypes
            import ibis.backends.bigquery

            # Try to match the error message from the google cloud bigquery library matching Arrow type error.
            # If the error message matches, requries to get the schema from the result and generate a empty pandas dataframe with the mapped schema
            #
            # It's a workaround for the issue that the ibis library does not support empty result for some special types (e.g. JSON or Interval)
            # see details:
            # - https://github.com/Canner/wren-engine/issues/909
            # - https://github.com/ibis-project/ibis/issues/10612
            if "Must pass schema" in str(e):
                credits_json = loads(
                    base64.b64decode(
                        self.connection_info.credentials.get_secret_value()
                    ).decode("utf-8")
                )
                credentials = service_account.Credentials.from_service_account_info(
                    credits_json
                )
                client = bigquery.Client(credentials=credentials)
                ibis_schema_mapper = ibis.backends.bigquery.BigQuerySchema()
                bq_fields = client.query(sql).result()
                ibis_fields = ibis_schema_mapper.to_ibis(bq_fields.schema)
                return pd.DataFrame(columns=ibis_fields.names)
            else:
                raise e


class DuckDBConnector:
    def __init__(self, _connection_info: ConnectionInfo):
        import duckdb

        self.connection = duckdb.connect()

    def query(self, sql: str, limit: int) -> pd.DataFrame:
        return self.connection.execute(sql).fetch_df().head(limit)

    def dry_run(self, sql: str) -> None:
        self.connection.execute(sql)


@cache
def _get_pg_type_names(connection: BaseBackend) -> dict[int, str]:
    cur = connection.raw_sql("SELECT oid, typname FROM pg_type")
    return dict(cur.fetchall())


class QueryDryRunError(UnprocessableEntityError):
    pass
