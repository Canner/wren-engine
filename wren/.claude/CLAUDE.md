# CLAUDE.md — wren package

Standalone Python SDK and CLI for Wren Engine. Wraps `wren-core-py` (PyO3 bindings) + Ibis connectors into a single installable package.

## Package Structure

```
wren/
  src/wren/
    engine.py       — WrenEngine facade (transpile / query / dry_run / dry_plan)
    cli.py          — Typer CLI: wren query|dry-run|dry-plan|validate
    mdl/            — wren-core-py session context + manifest extraction helpers
      cte_rewriter.py — CTE-based query rewriting
      wren_dialect.py — Custom sqlglot dialect
    connector/      — Per-datasource Ibis connectors (factory.py + one file per source)
      factory.py    — DataSource → connector dispatch registry
      base.py       — Base connector interface
      ibis.py       — Shared Ibis-backed connector (trino, clickhouse, snowflake, athena)
      postgres.py, mysql.py, mssql.py, bigquery.py, duckdb.py, oracle.py,
      redshift.py, spark.py, databricks.py, canner.py
    model/
      data_source.py — DataSource enum + per-source ConnectionInfo factories
      error.py       — WrenError, ErrorCode, ErrorPhase
    memory/         — Optional LanceDB-backed semantic memory (requires `wren[memory]`)
      store.py      — LanceDB vector store operations
      schema_indexer.py — MDL schema embedding/indexing
      embeddings.py — Sentence transformer integration
      cli.py        — `wren memory` CLI subcommands
  tests/
    unit/           — test_engine.py, test_cte_rewriter.py, test_memory.py
    connectors/     — test_duckdb.py, test_postgres.py, test_mysql.py
    suite/          — Shared test helpers (manifests.py, query.py)
```

## Build & Development

```bash
cd wren
just install          # build wren-core-py wheel + uv sync
just install-all      # with all optional extras (including memory)
just install-extra <extra>   # e.g. just install-extra postgres
just install-memory   # install memory extra (lancedb + sentence-transformers)
just dev              # run `wren` CLI
just test             # pytest tests/
just test-memory      # memory-specific tests
just lint             # ruff format --check + ruff check
just format           # ruff auto-fix (also aliased as `just fmt`)
just build            # uv build (produces wheel)
```

Uses `uv` (not Poetry). `pyproject.toml` uses `hatchling` as build backend.

## Key Design Points

- **WrenEngine** is the main entry point. It accepts a base64-encoded MDL JSON string, a `DataSource`, and a connection dict.
- **Query flow**: `_plan()` → wren-core `SessionContext.transform_sql()` → `_transpile()` via sqlglot → connector `.query()`.
- **Manifest extraction**: `_plan()` tries to extract a minimal sub-manifest scoped to the query's referenced tables before calling wren-core — this reduces planning overhead. Falls back to the full manifest on error.
- **`get_session_context` is `@cache`-decorated** — same `(manifest_str, function_path, properties, data_source)` tuple reuses the same SessionContext. Avoid mutating session state.
- **Write dialect mapping**: `canner` → `trino`; file sources (`local_file`, `s3_file`, `minio_file`, `gcs_file`) → `duckdb`. All others use `data_source.name` directly.
- **WrenEngine is a context manager** (`__enter__` / `__exit__` call `close()`).

## Connectors

`connector/factory.py` dispatches on `DataSource` to return the right connector. Each connector wraps an Ibis backend and exposes `.query(sql, limit)` and `.dry_run(sql)`. Base class in `connector/base.py`; Ibis-backed connectors share `connector/ibis.py`.

- **Dedicated modules**: `postgres.py`, `mysql.py`, `mssql.py`, `bigquery.py`, `duckdb.py`, `oracle.py` (native oracledb, not Ibis), `redshift.py`, `spark.py`, `databricks.py`, `canner.py`
- **Shared Ibis module** (`ibis.py`): trino, clickhouse, snowflake, athena
- **File connectors**: `local_file`, `s3_file`, `minio_file`, `gcs_file` all map to duckdb
- **doris** maps to mysql connector (MySQL-compatible protocol)
- **canner** maps to postgres connector

## Memory Module (Optional)

`wren/src/wren/memory/` — LanceDB-backed semantic memory for schema and query retrieval. Install via `wren[memory]`.

- **`WrenMemory`** — Main API: `index_manifest()`, `get_context()`, `store_query()`, `recall_queries()`, `describe_schema()`, `schema_is_current()`, `status()`, `reset()`
- Uses sentence-transformers for embedding MDL schema items and NL↔SQL query pairs
- CLI: `wren memory index|fetch|store|recall` subcommands (auto-registered when extras installed)
- Backing store: LanceDB (local or remote via opendal)

## Optional Extras

Install per data-source extras: `postgres`, `mysql`, `bigquery`, `snowflake`, `clickhouse`, `trino`, `mssql`, `databricks`, `redshift`, `spark`, `athena`, `oracle`, `memory`, `all`, `dev`.

On macOS, `mysql` extra needs:
```bash
PKG_CONFIG_PATH="$(brew --prefix mysql-client)/lib/pkgconfig" just install-extra mysql
```

## Dependency on wren-core-py

`wren-core-py` wheel is built locally from `../wren-core-py/` and installed via `--find-links`. Run `just build-core` (or `just install`) to rebuild after Rust changes.
