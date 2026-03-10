# CLAUDE.md

Wren Engine (OSS) — open-source semantic SQL engine for MCP clients and AI agents. Translates queries through MDL (Modeling Definition Language) against 22+ data sources, powered by Apache DataFusion (Canner fork).

## Repository Structure

- **wren-core/** — Rust semantic engine: MDL analysis, query planning, DataFusion integration
- **wren-core-base/** — Shared Rust crate: manifest types (`Model`, `Column`, `Metric`, `Relationship`, `View`)
- **wren-core-py/** — PyO3 Python bindings for wren-core, built with Maturin
- **ibis-server/** — FastAPI REST server: query execution, validation, 22 connector backends
- **mcp-server/** — MCP server exposing Wren Engine to AI agents
- **skills/** — User-facing MCP skills (wren-sql, wren-quickstart, etc.)

Supporting: `wren-core-legacy/` (Java fallback for v2), `mock-web-server/`, `benchmark/`, `example/`

## Build Quick Reference

| Module | Install | Test | Format / Lint |
|--------|---------|------|---------------|
| wren-core | — | `cargo test --lib --tests --bins` | `cargo fmt --all` / `cargo clippy` |
| wren-core-py | `just install` | `just test` | `just format` |
| ibis-server | `just install` | `just test <MARKER>` | `just format` / `just lint` |
| mcp-server | `uv sync` | — | — |

> Detailed commands, env vars, and test markers → see each module's README

## Architecture: Query Flow

```
SQL Query → ibis-server (FastAPI v3 router)
  → MDL Processing (manifest cache, validation)
  → wren-core-py (PyO3 FFI)
  → wren-core (Rust: MDL analysis → logical plan → optimization)
  → DataFusion (query planning)
  → Connector (Ibis/sqlglot → dialect SQL)
  → Native execution (Postgres, BigQuery, etc.)
```

Fallback: if wren-core (v3) fails, ibis-server retries via wren-core-legacy (Java, v2).

Key files: `ibis-server/app/routers/v3/connector.py`, `wren-core/core/src/logical_plan/analyze/`

## Conventions

- **Commits**: Conventional commits — `feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`, `perf:`, `deps:`
- **Releases**: Automated via release-please
- **Rust**: `cargo fmt`, `clippy -D warnings`, `taplo fmt` for TOML
- **Python**: `ruff` (line-length 88, Python 3.11 target), Poetry for deps
- **Snapshot tests**: wren-core uses `insta`
- **CI**: Rust CI on `wren-core/**`; ibis CI on all PRs; core-py CI on `wren-core-py/**` or `wren-core/**`
