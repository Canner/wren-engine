# Wren Engine Skill Reference

Skills are instruction files that extend AI agents with Wren-specific workflows. Install them into your local skills folder and invoke them by name during a conversation.

---

## generate-mdl

**File:** [generate-mdl/SKILL.md](generate-mdl/SKILL.md)

Generates a complete Wren MDL manifest by introspecting a live database through ibis-server — no local database drivers required.

### When to use

- Onboarding a new data source into Wren
- Scaffolding an MDL from an existing database schema
- Automating initial MDL setup as part of a data pipeline

### Required MCP tools

`setup_connection`, `list_remote_tables`, `list_remote_constraints`, `mdl_validate_manifest`, `mdl_save_project`, `deploy_manifest`

### Supported data sources

`POSTGRES`, `MYSQL`, `MSSQL`, `DUCKDB`, `BIGQUERY`, `SNOWFLAKE`, `CLICKHOUSE`, `TRINO`, `ATHENA`, `ORACLE`, `DATABRICKS`

### Workflow summary

1. Gather connection credentials from the user
2. Register the connection via `setup_connection`
3. Fetch table schema via `list_remote_tables`
4. Fetch foreign key constraints via `list_remote_constraints`
5. Optionally sample data for ambiguous columns
6. Build the MDL JSON (models, columns, relationships)
7. Validate via `mdl_validate_manifest`
8. Optionally save as a YAML project (see `mdl-project`)
9. Deploy via `deploy_manifest`

---

## mdl-project

**File:** [mdl-project/SKILL.md](mdl-project/SKILL.md)

Manages Wren MDL manifests as human-readable YAML project directories — similar to dbt projects. Makes MDL version-control friendly by splitting the monolithic JSON into one YAML file per model.

### When to use

- Persisting an MDL to disk for version control (Git)
- Loading a saved YAML project back into a deployable MDL JSON
- Compiling a YAML project to `target/mdl.json` for deployment

### Project layout

```
my_project/
├── wren_project.yml       # Catalog, schema, data source
├── models/
│   ├── orders.yml         # One file per model (snake_case fields)
│   └── customers.yml
├── relationships.yml
├── views.yml
└── target/
    └── mdl.json           # Compiled output (camelCase, deployable)
```

### Key operations

| Operation | Description |
|-----------|-------------|
| **Save** | Convert MDL JSON → YAML project directory (camelCase → snake_case) |
| **Load** | Read YAML project → assemble MDL JSON dict (snake_case → camelCase) |
| **Build** | Load + write result to `target/mdl.json` |
| **Deploy** | Pass `target/mdl.json` to `deploy(mdl_file_path=...)` |

### Field mapping (YAML ↔ JSON)

| YAML (snake_case) | JSON (camelCase) |
|-------------------|------------------|
| `data_source` | `dataSource` |
| `table_reference` | `tableReference` |
| `is_calculated` | `isCalculated` |
| `not_null` | `notNull` |
| `is_primary_key` | `isPrimaryKey` |
| `primary_key` | `primaryKey` |
| `join_type` | `joinType` |

---

## wren-sql

**File:** [wren-sql/SKILL.md](wren-sql/SKILL.md)

Comprehensive SQL authoring and debugging guide for Wren Engine. Covers core query rules, filter strategies, supported types, aggregation, and links to topic-specific references.

### When to use

- Writing SQL queries against Wren Engine MDL models
- Debugging SQL errors across parsing, planning, transpiling, or execution stages
- Working with complex types (ARRAY, STRUCT, JSON/VARIANT)
- Writing date/time calculations or interval arithmetic
- Targeting BigQuery as a backend database

### Reference files

| File | Topic |
|------|-------|
| [references/correction.md](wren-sql/references/correction.md) | Error diagnosis and correction workflow |
| [references/datetime.md](wren-sql/references/datetime.md) | Date/time functions, intervals, epoch conversion |
| [references/types.md](wren-sql/references/types.md) | ARRAY, STRUCT, JSON/VARIANT/OBJECT types |
| [references/bigquery.md](wren-sql/references/bigquery.md) | BigQuery dialect quirks |

---

## wren-quickstart

**File:** [wren-quickstart/SKILL.md](wren-quickstart/SKILL.md)

Sets up Wren Engine MCP server via Docker and connects it to Claude Code (or another MCP client) over streamable-http.

### When to use

- Running Wren MCP in Docker (no local Python/uv install required)
- Configuring Claude Code MCP to connect to a containerized Wren Engine
- Setting up Wren MCP for Cline, Cursor, or VS Code MCP Extension
- Fixing `localhost` → `host.docker.internal` in connection info for Docker

### Workflow summary

1. Ask user for workspace mount path
2. Create `docker/.env` with `MDL_WORKSPACE`
3. `docker compose up -d`
4. Rewrite `localhost` → `host.docker.internal` in connection credentials
5. Add `wren` MCP server to Claude Code using streamable-http on port 9000
6. Run `health_check()` to verify

---

## Installing a skill

```bash
# Single skill
cp -r skills/generate-mdl ~/.claude/skills/

# All skills
cp -r skills/* ~/.claude/skills/
```

Then invoke in your AI client:

```
/generate-mdl
/mdl-project
/wren-sql
```
