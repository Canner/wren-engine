"""MDL generation tools — flat MCP tools for agentic MDL creation.

Registers six tools onto the shared FastMCP instance.  The calling AI agent
acts as the orchestrator: it connects to a database, explores the schema,
builds an MDL manifest, and validates it — all via direct tool calls, without
a secondary LLM layer.

Tools registered:

- ``mdl_connect_database``  — establish a SQLAlchemy connection for a session
- ``mdl_list_tables``       — list user tables in the connected database
- ``mdl_get_column_info``   — column metadata (type, nullable, PK, FK)
- ``mdl_get_column_stats``  — distinct count, null count, min/max for a column
- ``mdl_get_sample_data``   — fetch sample rows to understand data semantics
- ``mdl_validate_manifest`` — validate an MDL dict (JSON Schema + dry-plan)

Requires ``sqlalchemy`` and ``jsonschema`` to be installed::

    uv pip install sqlalchemy jsonschema

Plus the appropriate DB driver, e.g.:
    uv pip install psycopg2-binary      # PostgreSQL
    uv pip install pymysql              # MySQL
    uv pip install duckdb-engine        # DuckDB

If ``sqlalchemy`` or ``jsonschema`` are missing the module loads silently and
no tools are registered, so the rest of the server
continues to work unchanged.

Environment variables:

- ``WREN_ENGINE_ENDPOINT`` — ibis-server URL for MDL dry-plan validation
"""

from __future__ import annotations

import base64
import json
import os
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

try:
    import sqlalchemy as sa
    from sqlalchemy import inspect as sa_inspect, text
    import jsonschema

    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False

# ---------------------------------------------------------------------------
# Module-level session state: session_id → SA Engine
# ---------------------------------------------------------------------------

_engines: dict[str, "sa.Engine"] = {}

# MDL JSON Schema — fetched once per process
_MDL_SCHEMA_URL = (
    "https://raw.githubusercontent.com/Canner/WrenAI/main/wren-mdl/mdl.schema.json"
)
_cached_schema: dict[str, Any] | None = None


def _get_mdl_schema() -> dict[str, Any]:
    global _cached_schema
    if _cached_schema is None:
        import httpx  # already in core deps
        resp = httpx.get(_MDL_SCHEMA_URL, timeout=10.0)
        resp.raise_for_status()
        _cached_schema = resp.json()
    return _cached_schema


def _engine(session_id: str) -> "sa.Engine | None":
    return _engines.get(session_id)


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


def register_mdl_tools(mcp: FastMCP) -> None:
    """Register flat MDL tools and the generate_mdl prompt onto *mcp*."""
    if not _AVAILABLE:
        print(  # noqa: T201
            "[MDL Tools] sqlalchemy or jsonschema not installed — "
            "MDL tools not registered. Run: uv pip install sqlalchemy jsonschema"
        )
        return

    # ------------------------------------------------------------------
    # Prompt: defines the MDL generation workflow as a reusable starting point.
    # Claude Desktop exposes this as a slash command: /generate_mdl
    # Other clients can invoke it via prompts/get.
    # ------------------------------------------------------------------

    @mcp.prompt()
    def generate_mdl(connection_string: str = "") -> str:
        """Guided workflow for generating a Wren MDL manifest from a database.

        Invoke this prompt to start the MDL generation process.  The agent
        will follow the steps below automatically.

        Args:
            connection_string: Optional SQLAlchemy URL to skip the first question.
                               e.g. ``postgresql://user:pass@host:5432/db``
        """
        conn_step = (
            f'You already have the connection string: `{connection_string}`\n'
            f'Start at Step 2.'
            if connection_string
            else 'Ask the user for their database connection string (SQLAlchemy URL).'
        )
        return f"""You are generating a Wren MDL manifest for a user's database.
Follow these steps in order — do not skip steps or ask unnecessary questions.

## Step 1 — Get connection string
{conn_step}

## Step 2 — Connect
Call `mdl_connect_database` with the connection string.
If it fails, report the error and ask the user to correct it.

## Step 3 — Explore schema
For EVERY table returned:
  a. Call `mdl_get_column_info(<table>)` to get column types, PKs, FKs.
  b. Call `mdl_get_sample_data(<table>, limit=3)` to understand data semantics.
  c. For columns where purpose is unclear, call `mdl_get_column_stats(<table>, <column>)`.

## Step 4 — Build MDL manifest
Construct the MDL JSON following these rules:
- `catalog`: use "wren" unless user specifies otherwise.
- `schema`: use the database's default schema (e.g. "public" for PostgreSQL).
- `dataSource`: set to the correct enum value (POSTGRES, MYSQL, DUCKDB, BIGQUERY, etc.).
- Each table → one `Model`. Set `tableReference.table` to the table name.
- Each column → one `Column`. Use the exact DB column name for `name`.
- Mark primary key columns with `"isHidden": false` and note them in model `primaryKey`.
- For FK columns, add a `Relationship` entry between the two models.
- Calculated columns and metrics can be added later — omit them for now.

## Step 5 — Validate
Call `mdl_validate_manifest` with the constructed manifest dict.
If validation fails, fix the reported errors and validate again.

## Step 6 — Deploy
Call `deploy` with the validated manifest.
Confirm success to the user.

Do not ask for confirmation between steps unless you encounter an error.
"""

    # ------------------------------------------------------------------
    # 1. Connect to database
    # ------------------------------------------------------------------

    @mcp.tool(
        annotations=ToolAnnotations(
            title="Connect to Database",
            readOnlyHint=False,
        ),
    )
    async def mdl_connect_database(
        connection_string: str,
        session_id: str = "default",
    ) -> str:
        """Connect to a database using a SQLAlchemy connection string.

        Must be called before any other ``mdl_*`` tool that needs schema access.
        Each ``session_id`` maintains its own independent connection.

        Supported schemes (driver must be installed separately):
        - ``postgresql://user:pass@host:5432/db``  (needs psycopg2-binary)
        - ``mysql+pymysql://user:pass@host:3306/db`` (needs pymysql)
        - ``duckdb:///path/to/file.db``             (needs duckdb-engine)
        - ``sqlite:///path/to/file.db``             (built-in)

        Args:
            connection_string: SQLAlchemy URL.
            session_id: Connection identifier, allows multiple parallel sessions.

        Returns:
            Success message with table count, or an error description.
        """
        try:
            engine = sa.create_engine(connection_string)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            _engines[session_id] = engine
            tables = sorted(sa_inspect(engine).get_table_names())
            preview = ", ".join(tables[:20]) + ("…" if len(tables) > 20 else "")
            return (
                f"Connected. Found {len(tables)} table(s): {preview}\n"
                f"→ Next: call `mdl_get_column_info` for each table, "
                f"then `mdl_get_sample_data` to understand the data."
            )
        except Exception as e:
            return f"Connection failed: {e}"

    # ------------------------------------------------------------------
    # 2. List tables
    # ------------------------------------------------------------------

    @mcp.tool(
        annotations=ToolAnnotations(
            title="List Database Tables",
            readOnlyHint=True,
        ),
    )
    async def mdl_list_tables(session_id: str = "default") -> str:
        """List all user tables in the connected database.

        Call ``mdl_connect_database`` first.

        Args:
            session_id: Session to query.

        Returns:
            JSON array of table names, or an error string.
        """
        engine = _engine(session_id)
        if engine is None:
            return f"No connection for session '{session_id}'. Call mdl_connect_database first."
        try:
            tables = sorted(sa_inspect(engine).get_table_names())
            return (
                json.dumps(tables) + "\n"
                "→ Next: call `mdl_get_column_info(<table>)` for each table."
            )
        except Exception as e:
            return f"Error listing tables: {e}"

    # ------------------------------------------------------------------
    # 3. Column info
    # ------------------------------------------------------------------

    @mcp.tool(
        annotations=ToolAnnotations(
            title="Get Column Info",
            readOnlyHint=True,
        ),
    )
    async def mdl_get_column_info(
        table: str,
        session_id: str = "default",
    ) -> str:
        """Get column metadata for a table.

        Returns name, data type, nullable flag, whether it is a primary key,
        and foreign key references (if any).

        Args:
            table: Table name.
            session_id: Session to query.

        Returns:
            JSON array of column objects, or an error string.
        """
        engine = _engine(session_id)
        if engine is None:
            return f"No connection for session '{session_id}'. Call mdl_connect_database first."
        try:
            insp = sa_inspect(engine)
            columns = insp.get_columns(table)
            pk_cols = set(insp.get_pk_constraint(table).get("constrained_columns", []))
            fk_map: dict[str, dict] = {}
            for fk in insp.get_foreign_keys(table):
                for local, ref in zip(fk["constrained_columns"], fk["referred_columns"]):
                    fk_map[local] = {"table": fk["referred_table"], "column": ref}

            result = []
            for col in columns:
                name = col["name"]
                entry: dict[str, Any] = {
                    "name": name,
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "primary_key": name in pk_cols,
                }
                if name in fk_map:
                    entry["foreign_key"] = fk_map[name]
                result.append(entry)
            return (
                json.dumps(result, default=str) + "\n"
                f"→ Next: call `mdl_get_sample_data('{table}', limit=3)` "
                f"to understand data semantics."
            )
        except Exception as e:
            return f"Error getting column info for '{table}': {e}"

    # ------------------------------------------------------------------
    # 4. Column stats
    # ------------------------------------------------------------------

    @mcp.tool(
        annotations=ToolAnnotations(
            title="Get Column Statistics",
            readOnlyHint=True,
        ),
    )
    async def mdl_get_column_stats(
        table: str,
        column: str,
        session_id: str = "default",
    ) -> str:
        """Get basic statistics for a column: distinct count, null count, min, max.

        Useful for inferring column semantics (e.g., ID vs. enum vs. metric).

        Args:
            table: Table name.
            column: Column name.
            session_id: Session to query.

        Returns:
            JSON object with stats, or an error string.
        """
        engine = _engine(session_id)
        if engine is None:
            return f"No connection for session '{session_id}'. Call mdl_connect_database first."
        try:
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
            return json.dumps({
                "distinct_count": row.distinct_count,
                "null_count": row.null_count,
                "min": str(row.min_val) if row.min_val is not None else None,
                "max": str(row.max_val) if row.max_val is not None else None,
            }, default=str)
        except Exception as e:
            return f"Error getting stats for '{table}.{column}': {e}"

    # ------------------------------------------------------------------
    # 5. Sample data
    # ------------------------------------------------------------------

    @mcp.tool(
        annotations=ToolAnnotations(
            title="Get Sample Data",
            readOnlyHint=True,
        ),
    )
    async def mdl_get_sample_data(
        table: str,
        limit: int = 5,
        session_id: str = "default",
    ) -> str:
        """Fetch sample rows from a table to understand data semantics.

        Args:
            table: Table name.
            limit: Number of rows (default 5, capped at 20).
            session_id: Session to query.

        Returns:
            JSON array of row objects, or an error string.
        """
        engine = _engine(session_id)
        if engine is None:
            return f"No connection for session '{session_id}'. Call mdl_connect_database first."
        try:
            limit = min(int(limit), 20)
            tbl = sa.table(table)
            stmt = sa.select(text("*")).select_from(tbl).limit(limit)
            with engine.connect() as conn:
                rows = conn.execute(stmt).mappings().all()
            return (
                json.dumps([dict(r) for r in rows], default=str) + "\n"
                "→ After exploring all tables: build the MDL manifest, "
                "then call `mdl_validate_manifest`."
            )
        except Exception as e:
            return f"Error sampling '{table}': {e}"

    # ------------------------------------------------------------------
    # 6. Validate MDL manifest
    # ------------------------------------------------------------------

    @mcp.tool(
        annotations=ToolAnnotations(
            title="Validate MDL Manifest",
            readOnlyHint=True,
        ),
    )
    async def mdl_validate_manifest(mdl: dict) -> str:
        """Validate an MDL manifest dict against the official Wren JSON Schema.

        Optionally runs a dry-plan against ibis-server when
        ``WREN_ENGINE_ENDPOINT`` is set.

        Pass the MDL as a plain dict (use ``"schema"`` key, not ``"schema_name"``).

        Args:
            mdl: The MDL manifest as a dictionary.

        Returns:
            A string describing the validation result.
        """
        # Step 1: JSON Schema
        try:
            schema = _get_mdl_schema()
        except Exception as e:
            return f"Warning: could not fetch MDL schema: {e}. Skipping schema check."

        errors = list(jsonschema.Draft202012Validator(schema).iter_errors(mdl))
        if errors:
            messages = "; ".join(
                f"{'.'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
                for e in errors[:5]
            )
            return f"JSON Schema validation failed ({len(errors)} error(s)): {messages}"

        # Step 2: ibis-server dry-plan (optional)
        import httpx  # already in core deps

        ibis_url = os.getenv("WREN_ENGINE_ENDPOINT")
        if not ibis_url:
            return (
                "JSON Schema validation passed. "
                "(Skipping dry-plan: WREN_ENGINE_ENDPOINT not set)\n"
                "→ Next: call `deploy` with this manifest."
            )

        try:
            manifest_str = base64.b64encode(json.dumps(mdl).encode()).decode()
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{ibis_url.rstrip('/')}/v3/connector/dry-plan",
                    json={"manifest_str": manifest_str, "sql": "SELECT 1"},
                    headers={"x-wren-fallback_disable": "true"},
                )
            if resp.status_code == 200:
                return (
                    "MDL validation passed (JSON Schema + dry-plan).\n"
                    "→ Next: call `deploy` with this manifest."
                )
            return (
                f"JSON Schema passed, but dry-plan failed "
                f"(HTTP {resp.status_code}): {resp.text[:300]}"
            )
        except Exception as e:
            return f"JSON Schema passed, but dry-plan request failed: {e}"
