from contextlib import closing
from functools import cache
from typing import Any

import ibis
import ibis.expr.schema as sch
import pyarrow as pa
from ibis import BaseBackend
from ibis.backends.sql.compilers.postgres import compiler as postgres_compiler
from ibis.expr.datatypes import Decimal
from ibis.expr.datatypes.core import UUID
from ibis.expr.types import Table
from loguru import logger

from wren.connector.base import ConnectorABC
from wren.model.data_source import DataSource


@cache
def _get_pg_type_names(connection: BaseBackend) -> dict[int, str]:
    with closing(connection.raw_sql("SELECT oid, typname FROM pg_type")) as cur:
        return dict(cur.fetchall())


class CannerConnector(ConnectorABC):
    def __init__(self, connection_info):
        self.connection = DataSource.canner.get_connection(connection_info)

    def query(self, sql: str, limit: int | None = None) -> pa.Table:
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
                col = result_table[name]
                decimal_type = Decimal(precision=38, scale=9)
                rounded_col = col.cast(decimal_type).round(9)
                result_table = result_table.mutate(**{name: rounded_col})
            elif isinstance(dtype, UUID):
                result_table = result_table.mutate(
                    **{name: result_table[name].cast("string")}
                )
        return result_table

    def dry_run(self, sql: str) -> Any:
        return self.connection.raw_sql(f"SELECT * FROM ({sql}) LIMIT 0")

    def close(self) -> None:
        try:
            if hasattr(self.connection, "con") and hasattr(
                self.connection.con, "close"
            ):
                self.connection.con.close()
            elif hasattr(self.connection, "close"):
                self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing Canner connection: {e}")

    def _get_schema(self, sql: str) -> sch.Schema:
        cur = self.dry_run(sql)
        type_names = _get_pg_type_names(self.connection)
        return ibis.schema(
            {
                desc.name: postgres_compiler.type_mapper.from_string(
                    type_names[desc.type_code]
                )
                for desc in cur.description
            }
        )


def create_connector(connection_info) -> CannerConnector:
    return CannerConnector(connection_info)
