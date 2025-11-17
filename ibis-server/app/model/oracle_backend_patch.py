"""
Patched Oracle backend for ibis to fix ORA-00923 error in _get_schema_using_query.

The bug: ibis generates `nullable = 'Y'` in the SELECT list which Oracle cannot parse.
The fix: Use CASE WHEN expression instead of boolean equality in SELECT list.
"""

import logging
from typing import TYPE_CHECKING

import ibis.expr.schema as sch
import sqlglot as sg
import sqlglot.expressions as sge
from ibis.backends.oracle import Backend as OracleBackend
from ibis.util import gen_name

if TYPE_CHECKING:
    import oracledb

logger = logging.getLogger(__name__)


class PatchedOracleBackend(OracleBackend):
    """Patched Oracle backend that fixes the metadata query bug."""

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        """
        Override the buggy ibis method that generates invalid SQL for Oracle.

        Original bug: Generates `nullable = 'Y'` which causes ORA-00923.
        Fix: Use CASE WHEN expression to create boolean column.
        """
        dialect = self.name

        with self.begin() as con:
            # Parse and transform the query
            sg_expr = sg.parse_one(query, dialect=dialect)

            # Apply ibis's transformer to add quoting
            transformer = lambda node: (
                node.__class__(this=node.this, quoted=True)
                if isinstance(node, sg.exp.Column)
                and not node.this.quoted
                and not isinstance(node.parent, sg.exp.Order)
                else node
            )
            sg_expr = sg_expr.transform(transformer)

            # Generate a random view name
            name = gen_name("oracle_metadata")
            this = sg.table(name, quoted=True)

            # Create the VIEW
            create_view = sg.exp.Create(
                kind="VIEW", this=this, expression=sg_expr
            ).sql(dialect)

            logger.debug(f"Creating temp view: {create_view}")
            con.execute(create_view)

            try:
                # PATCHED: Build metadata query with CASE WHEN instead of boolean equality
                # Original ibis code:
                #   C.nullable.eq(sge.convert("Y"))
                # This generates: nullable = 'Y' which Oracle can't parse in SELECT list

                # Fixed version: Use raw SQL with CASE WHEN
                metadata_sql = f"""
                    SELECT
                        column_name,
                        data_type,
                        data_precision,
                        data_scale,
                        CASE WHEN nullable = 'Y' THEN 1 ELSE 0 END as is_nullable
                    FROM all_tab_columns
                    WHERE table_name = '{name}'
                    ORDER BY column_id
                """

                logger.debug(f"Querying metadata: {metadata_sql}")
                results = con.execute(metadata_sql).fetchall()

                # Build schema from results
                type_mapper = self.compiler.type_mapper
                schema = {}

                for col_name, data_type, precision, scale, is_nullable in results:
                    # Map Oracle type to ibis type
                    # from_string() only accepts the type name and nullable parameter
                    # For schema inference, we just use the base type without precision/scale
                    # The actual precision/scale will be handled by the database
                    nullable_val = bool(is_nullable) if is_nullable is not None else True
                    ibis_type = type_mapper.from_string(data_type, nullable=nullable_val)
                    schema[col_name] = ibis_type

                return sch.Schema(schema)

            finally:
                # Always clean up the temp view
                drop_view = f'DROP VIEW "{name}"'
                logger.debug(f"Dropping temp view: {drop_view}")
                con.execute(drop_view)


def patch_oracle_backend():
    """
    Monkey-patch ibis to use our fixed Oracle backend.
    Call this before creating any Oracle connections.
    """
    import ibis

    # Replace the Oracle backend class
    original_backend = ibis.backends.oracle.Backend
    ibis.backends.oracle.Backend = PatchedOracleBackend

    logger.info("Applied Oracle backend patch to fix ORA-00923 error")

    return original_backend  # Return original in case we need to restore it
