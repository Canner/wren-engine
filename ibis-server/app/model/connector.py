from functools import cache
from typing import Any

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import pandas as pd
from ibis import BaseBackend
from ibis.backends.sql.compilers.postgres import compiler as postgres_compiler

from app.model import ConnectionInfo, UnprocessableEntityError
from app.model.data_source import DataSource


class Connector:
    def __init__(self, data_source: DataSource, connection_info: ConnectionInfo):
        if data_source == DataSource.canner:
            self._connector = CannerConnector(connection_info)
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


class CannerConnector:
    def __init__(self, connection_info: ConnectionInfo):
        self.connection = DataSource.canner.get_connection(connection_info)

    def query(self, sql: str, limit: int) -> pd.DataFrame:
        # Canner enterprise does not support `CREATE TEMPORARY VIEW` for getting schema
        schema = self._get_schema(sql)
        return self.connection.sql(sql, schema=schema).limit(limit).to_pandas()

    # Canner enterprise does not support dry-run, so we have to query with limit zero
    def dry_run(self, sql: str) -> Any:
        return self.connection.raw_sql(f"SELECT * FROM ({sql}) LIMIT 0")

    def _get_schema(self, sql: str) -> sch.Schema:
        cur = self.dry_run(sql)
        type_names = _get_pg_type_names(self.connection)
        return ibis.schema(
            {
                desc.name: self._oid_to_ibis_type(desc.type_code, type_names)
                for desc in cur.description
            }
        )

    @staticmethod
    def _oid_to_ibis_type(oid: int, type_names: dict[int, str]) -> dt.DataType:
        return postgres_compiler.type_mapper.from_string(type_names[oid])


@cache
def _get_pg_type_names(connection: BaseBackend) -> dict[int, str]:
    cur = connection.raw_sql("SELECT oid, typname FROM pg_type")
    return dict(cur.fetchall())


class QueryDryRunError(UnprocessableEntityError):
    pass
