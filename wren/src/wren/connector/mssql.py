from contextlib import closing
from decimal import Decimal as PyDecimal

import pyarrow as pa
import sqlglot.expressions as sge
from ibis.expr.datatypes import Decimal
from ibis.expr.types import Table
from sqlglot import exp, parse_one

from wren.connector.base import IbisConnector
from wren.model.data_source import DataSource
from wren.model.error import DIALECT_SQL, ErrorCode, ErrorPhase, WrenError


class MSSqlConnector(IbisConnector):
    def __init__(self, connection_info):
        super().__init__(DataSource.mssql, connection_info)

    def query(self, sql: str, limit: int | None = None) -> pa.Table:
        sql = self._flatten_pagination_limit(sql)
        ibis_table = self.connection.sql(sql)
        if limit is not None:
            ibis_table = ibis_table.limit(limit)
        ibis_table = self._handle_pyarrow_unsupported_type(ibis_table)
        return self._round_decimal_columns(ibis_table)

    def _round_decimal_columns(self, ibis_table: Table, scale: int = 9) -> pa.Table:
        def round_decimal(val):
            if val is None:
                return None
            d = PyDecimal(str(val))
            return d.quantize(PyDecimal("1." + "0" * scale))

        decimal_columns = [
            name
            for name, dtype in ibis_table.schema().items()
            if isinstance(dtype, Decimal)
        ]
        if not decimal_columns:
            return ibis_table.to_pyarrow()

        pandas_df = ibis_table.to_pandas()
        for col_name in decimal_columns:
            pandas_df[col_name] = pandas_df[col_name].apply(round_decimal)
        return pa.Table.from_pandas(pandas_df)

    def _flatten_pagination_limit(
        self, sql_query: str, input_dialect: str = "tsql"
    ) -> str:
        try:
            parsed = parse_one(sql_query, dialect=input_dialect)
            if not isinstance(parsed, exp.Select) or not parsed.args.get("limit"):
                return sql_query

            from_clause = parsed.find(exp.From)
            if not from_clause:
                return sql_query

            subqueries = []
            if isinstance(from_clause.this, exp.Subquery):
                subqueries.append(from_clause.this)
            for join in parsed.args.get("joins") or []:
                if isinstance(join, exp.Join):
                    if isinstance(join.this, exp.Subquery):
                        subqueries.append(join.this)
                    if join.expression and isinstance(join.expression, exp.Subquery):
                        subqueries.append(join.expression)

            if len(subqueries) != 1:
                return sql_query

            inner = subqueries[0].this
            if not isinstance(inner, exp.Select):
                return sql_query

            inner.set("limit", exp.Limit(expression=parsed.args["limit"].expression))
            return inner.sql(dialect="tsql")
        except Exception:
            return sql_query

    def dry_run(self, sql: str) -> None:
        try:
            super().dry_run(sql)
        except AttributeError as e:
            if "NoneType" in str(e) and "lower" in str(e):
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

    def _describe_sql_for_error_message(self, sql: str) -> str:
        try:
            tsql = sge.convert(sql).sql("mssql")
            describe_sql = f"SELECT error_message FROM sys.dm_exec_describe_first_result_set({tsql}, NULL, 0)"
            with closing(self.connection.raw_sql(describe_sql)) as cur:
                rows = cur.fetchall()
                if not rows:
                    return "Unknown reason"
                return rows[0][0]
        except Exception:
            return "Unknown reason"


def create_connector(connection_info) -> MSSqlConnector:
    return MSSqlConnector(connection_info)
