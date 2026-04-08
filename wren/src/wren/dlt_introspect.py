"""DuckDB schema introspection for dlt-produced databases.

Connects READ_ONLY to a .duckdb file produced by a dlt pipeline,
discovers user tables across all schemas, filters dlt internal tables
and columns, and detects parent-child relationships from dlt's
``_dlt_parent_id`` naming convention.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# dlt internal table names to exclude (exact match)
_DLT_INTERNAL_TABLES = frozenset({"_dlt_loads", "_dlt_pipeline_state", "_dlt_version"})

# dlt internal column names to exclude from model output
# (still read internally for relationship detection)
_DLT_INTERNAL_COLUMNS = frozenset(
    {"_dlt_id", "_dlt_parent_id", "_dlt_load_id", "_dlt_list_idx"}
)


@dataclass
class DltColumn:
    name: str
    data_type: str  # raw DuckDB type string
    normalized_type: str  # after sqlglot parse_type()
    is_nullable: bool


@dataclass
class DltTable:
    catalog: str  # attached database alias (filename stem)
    schema: str  # DuckDB schema (e.g. "main", "hubspot_crm")
    name: str  # table name
    columns: list[DltColumn] = field(default_factory=list)
    has_dlt_parent_id: bool = False


class DltIntrospector:
    """Introspect a dlt-produced DuckDB file and return tables + relationships."""

    def __init__(self, duckdb_path: str | Path):
        """Open the DuckDB file READ_ONLY via ATTACH."""
        import duckdb  # noqa: PLC0415

        self._path = Path(duckdb_path)
        if not self._path.exists():
            raise FileNotFoundError(f"DuckDB file not found: {self._path}")

        # Use the filename stem as the catalog name for table_reference output,
        # but prefix the ATTACH alias to avoid collisions with DuckDB reserved
        # catalog names (memory, system, temp, etc.).
        self._catalog = self._path.stem
        self._alias = "dlt_" + self._catalog
        self._con = duckdb.connect()
        escaped_path = str(self._path).replace("'", "''")
        escaped_alias = self._alias.replace('"', '""')
        self._con.execute(
            f"ATTACH '{escaped_path}' AS \"{escaped_alias}\" (READ_ONLY)"
        )

    # ── Public API ─────────────────────────────────────────────────────────

    def introspect(self) -> tuple[list[DltTable], list[dict]]:
        """Return (tables, relationships) for the attached database."""
        tables = self._discover_tables()
        relationships = self._detect_relationships(tables)
        return tables, relationships

    # ── Internal helpers ───────────────────────────────────────────────────

    def _discover_tables(self) -> list[DltTable]:
        """Query duckdb_tables() / duckdb_columns() and build DltTable list."""
        from wren.type_mapping import parse_type  # noqa: PLC0415

        # duckdb_tables() returns (database_name, schema_name, table_name, ...)
        table_rows = self._con.execute(
            """
            SELECT schema_name, table_name
            FROM duckdb_tables()
            WHERE database_name = ?
              AND NOT starts_with(table_name, '_dlt_')
            ORDER BY schema_name, table_name
        """,
            [self._alias],
        ).fetchall()

        tables: list[DltTable] = []
        for schema, table_name in table_rows:
            if table_name in _DLT_INTERNAL_TABLES:
                continue

            # duckdb_columns() returns (database_name, schema_name, table_name,
            # column_name, data_type, is_nullable, ...)
            col_rows = self._con.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM duckdb_columns()
                WHERE database_name = ? AND schema_name = ? AND table_name = ?
                ORDER BY column_index
            """,
                [self._alias, schema, table_name],
            ).fetchall()

            has_parent_id = False
            columns: list[DltColumn] = []
            for col_name, raw_type, is_nullable in col_rows:
                if col_name == "_dlt_parent_id":
                    has_parent_id = True
                if col_name in _DLT_INTERNAL_COLUMNS:
                    continue
                normalized = parse_type(raw_type, "duckdb")
                columns.append(
                    DltColumn(
                        name=col_name,
                        data_type=raw_type,
                        normalized_type=normalized,
                        is_nullable=bool(is_nullable),
                    )
                )

            tables.append(
                DltTable(
                    catalog=self._catalog,
                    schema=schema,
                    name=table_name,
                    columns=columns,
                    has_dlt_parent_id=has_parent_id,
                )
            )

        return tables

    def _detect_relationships(self, tables: list[DltTable]) -> list[dict]:
        """Detect parent-child relationships from dlt's ``__`` naming convention.

        A child table has ``_dlt_parent_id`` and its name is
        ``{parent_name}__{child_suffix}``.  We find the longest prefix of
        the child's ``__``-split parts that matches a known table name.
        """
        table_names = {t.name for t in tables}
        relationships: list[dict] = []

        for table in tables:
            if not table.has_dlt_parent_id:
                continue

            parts = table.name.split("__")
            parent_name: str | None = None
            for i in range(len(parts) - 1, 0, -1):
                candidate = "__".join(parts[:i])
                if candidate in table_names:
                    parent_name = candidate
                    break

            if parent_name is None:
                logger.warning(
                    "Child table '%s' has _dlt_parent_id but no matching parent "
                    "found — skipping relationship",
                    table.name,
                )
                continue

            child_suffix = table.name[len(parent_name) + 2 :]  # strip "parent__"
            rel_name = f"{parent_name}_{child_suffix}"
            relationships.append(
                {
                    "name": rel_name,
                    "models": [parent_name, table.name],
                    "join_type": "ONE_TO_MANY",
                    "condition": (
                        f"{table.name}._dlt_parent_id = {parent_name}._dlt_id"
                    ),
                }
            )

        return relationships

    # ── Context manager ────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._con.close()

    def __enter__(self) -> DltIntrospector:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
