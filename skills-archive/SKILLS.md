# Wren Engine Skill Reference

Skills are instruction files that extend AI agents with Wren-specific workflows. Install them into your local skills folder and invoke them by name during a conversation.

---

## wren-usage

**File:** [wren-usage/SKILL.md](wren-usage/SKILL.md)

**Primary entry point** for day-to-day Wren Engine usage. Identifies the user's task and delegates to the appropriate focused skill. Covers SQL queries, MDL management, database connections, and MCP server operations.

### When to use

- Writing or debugging SQL queries against a deployed MDL
- Adding or modifying models, columns, or relationships in the MDL
- Changing database credentials or data source
- Rebuilding `target/mdl.json` after project changes
- Restarting or reconfiguring the MCP server
- Any ongoing Wren task after initial setup is complete

### Dependent skills

| Skill | Purpose |
|-------|---------|
| `@wren-sql` | Write and debug SQL queries |
| `@wren-connection-info` | Set up or change database credentials |
| `@wren-generate-mdl` | Regenerate MDL from a changed database schema |
| `@wren-project` | Save, load, and build MDL YAML projects |
| `@wren-mcp-setup` | Reconfigure the MCP server |

> Installing `wren-usage` via `install.sh` automatically installs all dependent skills:
> ```bash
> bash skills/install.sh wren-usage
> ```

---

## wren-quickstart

**File:** [wren-quickstart/SKILL.md](wren-quickstart/SKILL.md)

End-to-end onboarding guide for Wren Engine. Orchestrates the full setup flow — from installing skills and creating a workspace, to generating an MDL, saving it as a versioned project, starting the MCP Docker container, and verifying everything works.

### When to use

- Setting up Wren Engine for the first time
- Onboarding a new data source from scratch
- Getting a new team member started with Wren MCP

### Workflow summary

1. Install required skills via `install.sh`
2. Create a workspace directory on the host machine
3. Generate MDL from the database (`@wren-generate-mdl`)
4. Save as a YAML project and compile to `target/` (`@wren-project`)
5. Start the Docker container and register the MCP server (`@wren-mcp-setup`)
6. Run `health_check()` to verify — then start a new session and query

### Dependent skills

| Skill | Purpose |
|-------|---------|
| `@wren-generate-mdl` | Introspect database and build MDL JSON |
| `@wren-project` | Save MDL as YAML project + compile to `target/` |
| `@wren-mcp-setup` | Start Docker container and register MCP server |

---

## wren-generate-mdl

**File:** [wren-generate-mdl/SKILL.md](wren-generate-mdl/SKILL.md)

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
8. Optionally save as a YAML project (see `wren-project`)
9. Deploy via `deploy_manifest`

---

## wren-project

**File:** [wren-project/SKILL.md](wren-project/SKILL.md)

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

## wren-mcp-setup

**File:** [wren-mcp-setup/SKILL.md](wren-mcp-setup/SKILL.md)

Sets up Wren Engine MCP server via Docker, registers it with an AI agent (Claude Code or other MCP clients), and starts a new session to begin interacting with Wren.

### When to use

- Running Wren MCP in Docker (no local Python/uv install required)
- Configuring Claude Code MCP to connect to a containerized Wren Engine
- Setting up Wren MCP for Cline, Cursor, or VS Code MCP Extension
- Fixing `localhost` → `host.docker.internal` in connection info for Docker

### Workflow summary

1. Ask user for workspace mount path
2. `docker run` with workspace mounted at `/workspace`, MCP server enabled on port 9000
3. Rewrite `localhost` → `host.docker.internal` in connection credentials
4. Add `wren` MCP server to Claude Code using streamable-http on port 9000 (`claude mcp add`)
5. Start a new session so the MCP tools are loaded
6. Run `health_check()` to verify

---

## wren-http-api

**File:** [wren-http-api/SKILL.md](wren-http-api/SKILL.md)

Interact with Wren Engine MCP server via plain HTTP JSON-RPC requests — no MCP client SDK required. Covers session initialization, tool discovery, and calling all 20+ Wren tools using standard HTTP POST with JSON-RPC 2.0 payloads.

### When to use

- The client cannot or prefers not to use the MCP protocol directly (e.g. OpenClaw)
- Building a custom HTTP integration with Wren Engine
- Calling Wren tools from shell scripts, CI pipelines, or non-MCP environments
- Debugging MCP tool calls with curl

### Workflow summary

1. Initialize a JSON-RPC session via `POST /mcp` with `initialize` method
2. Save the `Mcp-Session-Id` header from the response
3. Complete the handshake with `notifications/initialized`
4. Call any Wren tool via `tools/call` method with the session header
5. Parse SSE `data:` lines from responses

---

## Installing a skill

```bash
# Install wren-usage (auto-installs all dependencies)
bash skills/install.sh wren-usage

# Or install everything
bash skills/install.sh
```

Then invoke in your AI client:

```
/wren-usage
/wren-generate-mdl
/wren-project
/wren-sql
/wren-mcp-setup
/wren-quickstart
```
