"""Database exploration tools for the MDL agent."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from pydantic_ai import RunContext
from sqlalchemy import inspect, text

from wren_agent.deps import AgentDeps


def _require_engine(ctx: RunContext[AgentDeps]) -> sa.Engine | str:
    """Return the engine or an error string if not connected."""
    if ctx.deps.engine is None:
        return "No database connection. Please call connect_to_database first."
    return ctx.deps.engine


async def connect_to_database(
    ctx: RunContext[AgentDeps], connection_string: str
) -> str:
    """Establish a database connection using a SQLAlchemy connection string.

    Supported schemes: postgresql://, mysql+pymysql://, duckdb:///, and others.
    Example: postgresql://user:pass@localhost:5432/mydb

    Returns a success message listing available tables, or an error string.
    """
    try:
        engine = sa.create_engine(connection_string)
        # Eagerly verify connectivity
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        ctx.deps.engine = engine

        insp = inspect(engine)
        tables = insp.get_table_names()
        schema_tables = insp.get_table_names(schema=None)
        all_tables = sorted(set(tables + schema_tables))
        return (
            f"Connected successfully. Found {len(all_tables)} table(s): "
            + ", ".join(all_tables[:20])
            + ("..." if len(all_tables) > 20 else "")
        )
    except Exception as e:
        return f"Connection failed: {e}"


async def list_tables(ctx: RunContext[AgentDeps]) -> list[str] | str:
    """List all user tables in the connected database."""
    engine = _require_engine(ctx)
    if isinstance(engine, str):
        return engine
    try:
        insp = inspect(engine)
        return sorted(insp.get_table_names())
    except Exception as e:
        return f"Error listing tables: {e}"


async def get_column_info(
    ctx: RunContext[AgentDeps], table: str
) -> list[dict[str, Any]] | str:
    """Get column metadata for a table: name, type, nullable, primary key, foreign keys.

    Returns a list of dicts, one per column.
    """
    engine = _require_engine(ctx)
    if isinstance(engine, str):
        return engine
    try:
        insp = inspect(engine)
        columns = insp.get_columns(table)
        pk_constraint = insp.get_pk_constraint(table)
        pk_cols = set(pk_constraint.get("constrained_columns", []))
        fk_list = insp.get_foreign_keys(table)

        fk_map: dict[str, dict[str, str]] = {}
        for fk in fk_list:
            for local_col, ref_col in zip(
                fk["constrained_columns"], fk["referred_columns"]
            ):
                fk_map[local_col] = {
                    "referred_table": fk["referred_table"],
                    "referred_column": ref_col,
                }

        result = []
        for col in columns:
            col_name = col["name"]
            info: dict[str, Any] = {
                "name": col_name,
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "primary_key": col_name in pk_cols,
            }
            if col_name in fk_map:
                info["foreign_key"] = fk_map[col_name]
            result.append(info)
        return result
    except Exception as e:
        return f"Error getting column info for '{table}': {e}"


async def get_column_stats(
    ctx: RunContext[AgentDeps], table: str, column: str
) -> dict[str, Any] | str:
    """Get basic statistics for a column: distinct count, null count, min, max.

    Useful for inferring column semantics and data quality.
    """
    engine = _require_engine(ctx)
    if isinstance(engine, str):
        return engine
    try:
        # Use quoted identifiers to avoid SQL injection via table/column names
        col_id = sa.column(column)
        tbl = sa.table(table, col_id)
        stmt = sa.select(
            sa.func.count(sa.func.distinct(col_id)).label("distinct_count"),
            (sa.func.count() - sa.func.count(col_id)).label("null_count"),
            sa.func.min(col_id).label("min_val"),
            sa.func.max(col_id).label("max_val"),
        ).select_from(tbl)
        with engine.connect() as conn:
            row = conn.execute(stmt).one()
        return {
            "distinct_count": row.distinct_count,
            "null_count": row.null_count,
            "min": str(row.min_val) if row.min_val is not None else None,
            "max": str(row.max_val) if row.max_val is not None else None,
        }
    except Exception as e:
        return f"Error getting stats for '{table}.{column}': {e}"


async def get_sample_data(
    ctx: RunContext[AgentDeps], table: str, limit: int = 5
) -> list[dict[str, Any]] | str:
    """Fetch sample rows from a table to help infer column semantics.

    Args:
        table: Table name.
        limit: Number of rows to return (default 5, max 20).
    """
    engine = _require_engine(ctx)
    if isinstance(engine, str):
        return engine
    limit = min(limit, 20)
    try:
        tbl = sa.table(table)
        stmt = sa.select(sa.text("*")).select_from(tbl).limit(limit)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            keys = list(result.keys())
            rows = [dict(zip(keys, row)) for row in result.fetchall()]
        # Coerce non-serializable values to strings
        return [
            {k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v)
             for k, v in row.items()}
            for row in rows
        ]
    except Exception as e:
        return f"Error getting sample data for '{table}': {e}"
