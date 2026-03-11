# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Wren Engine (OSS) is an open source semantic engine for MCP clients and AI agents. It translates SQL queries through a semantic layer (MDL - Modeling Definition Language) and executes them against 22+ data sources (PostgreSQL, BigQuery, Snowflake, Spark, etc.). The engine is powered by Apache DataFusion (Canner fork).

## Repository Structure

Four main modules:

- **wren-core/** — Rust semantic engine (Cargo workspace: `core/`, `sqllogictest/`, `benchmarks/`, `wren-example/`). Handles MDL analysis, query planning, logical plan optimization, and SQL generation via DataFusion.
- **wren-core-base/** — Shared Rust crate with manifest types (`Model`, `Column`, `Metric`, `Relationship`, `View`). Has optional `python-binding` feature for PyO3 compatibility.
- **wren-core-py/** — PyO3 bindings exposing wren-core to Python. Built with Maturin.
- **ibis-server/** — FastAPI web server (Python 3.11). Provides REST API for query execution, validation, and metadata. Uses Ibis framework for data source connectivity.
- **mcp-server/** — MCP server exposing Wren Engine to AI agents (Claude, Cline, Cursor).

Supporting modules: `wren-core-legacy/` (Java engine, fallback for v2 queries), `mock-web-server/`, `benchmark/`, `example/`.

## Build & Development Commands

### wren-core (Rust)
```bash
cd wren-core
cargo check --all-targets           # Compile check
cargo test --lib --tests --bins     # Run tests (set RUST_MIN_STACK=8388608)
cargo fmt --all                     # Format Rust code
cargo clippy --all-targets --all-features -- -D warnings  # Lint
taplo fmt                           # Format Cargo.toml files
```

Most unit tests are in `wren-core/core/src/mdl/mod.rs`. SQL end-to-end tests use sqllogictest files in `wren-core/sqllogictest/test_files/`.

### wren-core-py (Python bindings)
```bash
cd wren-core-py
just install                        # Poetry install
just develop                        # Build dev wheel with maturin
just test-rs                        # Rust tests (cargo test --no-default-features)
just test-py                        # Python tests (pytest)
just test                           # Both
just format                         # cargo fmt + ruff + taplo
```

### ibis-server (FastAPI)
```bash
cd ibis-server
just install                        # Poetry install + build wren-core-py wheel + cython rebuild
just dev                            # Dev server on port 8000
just run                            # Production server on port 8000
just test <MARKER>                  # Run pytest with marker (e.g., just test postgres)
just lint                           # ruff format check + ruff check
just format                         # ruff auto-fix + taplo
```

Available test markers: `postgres`, `mysql`, `mssql`, `bigquery`, `snowflake`, `clickhouse`, `trino`, `oracle`, `athena`, `duckdb`, `athena_spark`, `databricks`, `spark`, `doris`, `local_file`, `s3_file`, `gcs_file`, `minio_file`, `functions`, `profile`, `cache`, `unit`, `enterprise`, `beta`.

### mcp-server
Uses `uv` for dependency management. See `mcp-server/README.md`.

### Docker (ibis-server image)

```bash
cd ibis-server
just docker-build                                      # current platform, Rust built locally
just docker-build linux/amd64                          # single specific platform
just docker-build linux/amd64,linux/arm64 --push       # multi-arch (must --push, cannot load locally)
```

**Two build strategies controlled by `WHEEL_SOURCE` build-arg:**

| Scenario | Strategy | Speed |
|----------|----------|-------|
| Target == host platform | `WHEEL_SOURCE=local` — Rust built on host via maturin+zig | Fast (reuses host cargo cache) |
| Cross-platform / multi-arch | `WHEEL_SOURCE=docker` — Rust built inside Docker via BuildKit cache mounts | Slow first build, incremental after |

`just docker-build` auto-detects host platform and chooses the right strategy.

**Prerequisites for local strategy (one-time setup):**
```bash
brew install zig
rustup target add aarch64-unknown-linux-gnu   # Apple Silicon
rustup target add x86_64-unknown-linux-gnu    # Intel Mac
```

**Key constraints:**
- Multi-arch builds always use `WHEEL_SOURCE=docker` (Rust compiled inside Docker)
- Multi-arch images cannot be loaded locally — `--push` to a registry is required
- BuildKit must be enabled (`DOCKER_BUILDKIT=1` or Docker Desktop default)
- Build contexts required: `wren-core-py`, `wren-core`, `wren-core-base`, `mcp-server` (all relative to `ibis-server/`)

## Architecture: Query Flow

```
SQL Query → ibis-server (FastAPI v3 router)
  → MDL Processing (manifest cache, validation)
  → wren-core-py (PyO3 FFI)
  → wren-core (Rust: MDL analysis → logical plan → optimization)
  → DataFusion (query planning)
  → Connector (data source-specific SQL via Ibis/sqlglot)
  → Native execution (Postgres, BigQuery, etc.)
  → Response (with optional query caching)
```

If wren-core (v3) fails, ibis-server falls back to the legacy Java engine (v2).

## Key Architecture Details

**wren-core internals** (`wren-core/core/src/`):
- `mdl/` — Core MDL processing: `WrenMDL` (manifest + symbol table), `AnalyzedWrenMDL` (with lineage), function definitions (scalar/aggregate/window per dialect), type planning
- `logical_plan/analyze/` — DataFusion analyzer rules: `ModelAnalyzeRule` (TableScan → ModelPlanNode), scope tracking, access control (RLAC/CLAC), view expansion, relationship chain resolution
- `logical_plan/optimize/` — Optimization passes: type coercion, timestamp simplification
- `sql/` — SQL parsing and analysis

**ibis-server internals** (`ibis-server/app/`):
- `routers/v3/connector.py` — Main API endpoints (query, validate, dry-plan, metadata)
- `model/metadata/` — Per-connector implementations (22 connectors), each with its own metadata handling
- `model/metadata/factory.py` — Connector instantiation
- `mdl/` — MDL processing: `core.py` (session context), `rewriter.py` (query rewriting), `substitute.py` (model substitution)
- `custom_ibis/`, `custom_sqlglot/` — Ibis and SQLGlot extensions for Wren-specific behavior

**Manifest types** (`wren-core-base/src/mdl/`):
- `manifest.rs` — `Manifest`, `Model`, `Column`, `Metric`, `Relationship`, `View`, `RowLevelAccessControl`, `ColumnLevelAccessControl`
- `builder.rs` — Fluent `ManifestBuilder` API
- Uses `wren-manifest-macro` for auto-generating Pydantic-compatible Python classes

## Running ibis-server Tests Locally

Required environment variables (see `.github/workflows/ibis-ci.yml` for CI values):
```bash
export QUERY_CACHE_STORAGE_TYPE=local
export WREN_ENGINE_ENDPOINT=http://localhost:8080
export WREN_WEB_ENDPOINT=http://localhost:3000
export PROFILING_STORE_PATH=file:///tmp/profiling
```

On macOS, `psycopg2` may fail to build due to missing OpenSSL linkage:
```bash
LDFLAGS="-L$(brew --prefix openssl)/lib" CPPFLAGS="-I$(brew --prefix openssl)/include" just install
```

Connector tests use testcontainers (Docker required). Example running a single connector:
```bash
just test clickhouse   # runs pytest -m clickhouse
```

TPCH test data is generated via DuckDB's TPCH extension (`CALL dbgen(sf=0.01)`) and loaded into the testcontainer at module scope. See `tests/routers/v3/connector/clickhouse/conftest.py` for the pattern.

## Known wren-core Limitations

**ModelAnalyzeRule — correlated subquery column resolution**: The `ModelAnalyzeRule` in `wren-core/core/src/logical_plan/analyze/` cannot resolve outer column references inside correlated subqueries. It only sees the subquery's own table scope. This affects TPCH Q2, Q4, Q15, Q17, Q20, Q21, Q22. See `ibis-server/tests/routers/v3/connector/clickhouse/TPCH_ISSUES.md`.

## Conventions

- **Commits**: Conventional commits (`feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`, `perf:`, `deps:`). Releases are automated via release-please.
- **Rust**: Format with `cargo fmt`, lint with `clippy -D warnings`, TOML formatting with `taplo`.
- **Python**: Format and lint with `ruff` (line-length 88, target Python 3.11). Poetry for dependency management.
- **DataFusion fork**: `https://github.com/Canner/datafusion.git` branch `canner/v49.0.1`. Also forked Ibis: `https://github.com/Canner/ibis.git` branch `canner/10.8.1`.
- **Snapshot testing**: wren-core uses `insta` for Rust snapshot tests.
- **CI**: Rust CI runs on `wren-core/**` changes. ibis CI runs on all PRs. Core-py CI runs on `wren-core-py/**` or `wren-core/**` changes.
