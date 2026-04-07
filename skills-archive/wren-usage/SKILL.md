---
name: wren-usage
description: "Wren Engine — semantic SQL engine for AI agents. Query 22+ data sources (PostgreSQL, BigQuery, Snowflake, MySQL, ClickHouse, etc.) through a modeling layer (MDL). This skill is the main entry point: it guides setup, delegates to focused sub-skills for SQL authoring, MDL generation, project management, and MCP server operations. Use when: write SQL, query data, generate or update MDL, change database connection, manage YAML projects, set up or operate MCP server, or get started with Wren Engine for the first time."
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.3"
---

# Wren Engine — Usage Guide

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-usage` key with this skill's version (from the frontmatter above).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-usage** skill is available.
> Update with:
> ```
> npx skills add Canner/wren-engine --skill wren-usage --agent claude-code
> ```

Then continue with the workflow below regardless of update status.

---

This skill is your day-to-day reference for working with Wren Engine. It delegates to focused sub-skills for each task.

---

## Step 0 — Install dependent skills (first time only)

Check whether the required skills are already installed in `~/.claude/skills/`. If any are missing, tell the user to run:

```bash
# Option A — npx skills (works with Claude Code, Cursor, and 30+ agents)
npx skills add Canner/wren-engine --skill '*' --agent claude-code

# Option B — Clawhub (if installed via clawhub)
clawhub install wren-usage
```

This installs `wren-usage` and its dependent skills (`wren-connection-info`, `wren-generate-mdl`, `wren-project`, `wren-sql`, `wren-mcp-setup`, `wren-http-api`) into `~/.claude/skills/`.

After installation, the user must **start a new session** for the new skills to be loaded.

> If the user only wants the MCP server set up (no Docker yet), use `/wren-quickstart` for a guided end-to-end walkthrough instead.

---

## What do you want to do?

Identify the user's intent and delegate to the appropriate skill:

| Task | Skill |
|------|-------|
| Write or debug a SQL query | `@wren-sql` |
| Connect to a new database / change credentials | `@wren-connection-info` |
| Generate MDL from an existing database | `@wren-generate-mdl` |
| Save MDL to YAML files (version control) | `@wren-project` |
| Load a saved YAML project / rebuild `target/mdl.json` | `@wren-project` |
| Add a new model or column to the MDL | `@wren-project` |
| Start, reset, or reconfigure the MCP server | `@wren-mcp-setup` |
| Call Wren tools via HTTP JSON-RPC (no MCP SDK) | `@wren-http-api` |
| First-time setup from scratch | `@wren-quickstart` |

---

## Common workflows

### Query your data

Invoke `@wren-sql` to write a SQL query against the deployed MDL.

Key rules:
- Query MDL model names directly (e.g. `SELECT * FROM orders`)
- Use `CAST` for type conversions, not `::` syntax
- Avoid correlated subqueries — use JOINs or CTEs instead

```sql
-- Example: revenue by month
SELECT DATE_TRUNC('month', order_date) AS month,
       SUM(total) AS revenue
FROM orders
GROUP BY 1
ORDER BY 1
```

For type-specific patterns (ARRAY, STRUCT, JSON), date/time arithmetic, or BigQuery dialect quirks, invoke `@wren-sql` for full guidance.

---

### Update connection credentials

To change credentials, direct the user to the MCP server Web UI at `http://localhost:9001`. Connection info can only be configured through the Web UI — do not attempt to set it programmatically.

Invoke `@wren-connection-info` for a reference of required fields per data source (so you can guide the user on what to enter in the Web UI).

---

### Extend the MDL

To add a model, column, relationship, or view to an existing project:

1. Invoke `@wren-project` — **Load** the existing YAML project into an MDL dict
2. Edit the relevant YAML file (e.g. `models/orders.yml`)
3. Invoke `@wren-project` — **Build** to compile updated `target/mdl.json`
4. Call `deploy(mdl_file_path="./target/mdl.json")` to apply the change

---

### Regenerate MDL from database

When the database schema has changed and the MDL needs to be refreshed:

1. Invoke `@wren-connection-info` — confirm or update credentials
2. Invoke `@wren-generate-mdl` — re-introspect the database and rebuild the MDL JSON
3. Invoke `@wren-project` — **Save** the new MDL as an updated YAML project
4. Invoke `@wren-project` — **Build** to compile `target/mdl.json`
5. Deploy

---

### MCP server operations

| Operation | Command |
|-----------|---------|
| Check status | `docker ps --filter name=wren-mcp` |
| View logs | `docker logs wren-mcp` |
| Restart | `docker restart wren-mcp` |
| Full reconfigure | Invoke `@wren-mcp-setup` |
| Verify health | `health_check()` via MCP tools |

---

## Quick reference — MCP tools

| Tool | Purpose |
|------|---------|
| `health_check()` | Verify Wren Engine is reachable |
| `query(sql=...)` | Execute a SQL query against the deployed MDL |
| `deploy(mdl_file_path=...)` | Load a compiled `mdl.json` |
| `setup_connection(...)` | Configure data source credentials |
| `list_remote_tables(...)` | Introspect database schema |
| `mdl_validate_manifest(...)` | Validate an MDL JSON dict |
| `mdl_save_project(...)` | Save MDL as a YAML project |

---

## Troubleshooting quick guide

**Query fails with "table not found":**
- The MDL may not be deployed. Run `deploy(mdl_file_path="./target/mdl.json")`.
- Check model names match exactly (case-sensitive).

**Connection error on queries:**
- Verify credentials with `@wren-connection-info`.
- Inside Docker: use `host.docker.internal` instead of `localhost`.

**MDL changes not reflected:**
- Re-run `@wren-project` **Build** step and re-deploy.

**MCP tools unavailable:**
- Start a new Claude Code session after registering the MCP server.
- Check: `docker ps --filter name=wren-mcp` and `docker logs wren-mcp`.

For detailed MCP setup troubleshooting, invoke `@wren-mcp-setup`.
