# CLAUDE.md — wren package

Standalone Python SDK and CLI for Wren Engine. Wraps `wren-core-py` (PyO3 bindings) + Ibis connectors into a single installable package.

## Package Structure

```
wren/
  src/wren/
    engine.py       — WrenEngine facade (transpile / query / dry_run / dry_plan)
    cli.py          — Typer CLI: wren query|dry-run|transpile|validate
    mdl/            — wren-core-py session context + manifest extraction helpers
    connector/      — Per-datasource Ibis connectors (factory.py + one file per source)
    model/
      data_source.py — DataSource enum + per-source ConnectionInfo factories
      error.py       — WrenError, ErrorCode, ErrorPhase
  tests/
```

## Build & Development

```bash
cd wren
just install          # build wren-core-py wheel + uv sync
just install-all      # with all optional extras
just install-extra <extra>   # e.g. just install-extra postgres
just test             # pytest tests/
just lint             # ruff format --check + ruff check
just format           # ruff auto-fix
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

## Optional Extras

Install per data-source extras: `postgres`, `mysql`, `bigquery`, `snowflake`, `clickhouse`, `trino`, `mssql`, `databricks`, `redshift`, `spark`, `athena`, `oracle`, `all`, `dev`.

On macOS, `mysql` extra needs:
```bash
PKG_CONFIG_PATH="$(brew --prefix mysql-client)/lib/pkgconfig" just install-extra mysql
```

## Dependency on wren-core-py

`wren-core-py` wheel is built locally from `../wren-core-py/` and installed via `--find-links`. Run `just build-core` (or `just install`) to rebuild after Rust changes.
