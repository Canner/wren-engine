---
name: wren-usage
description: Daily usage guide for Wren Engine — connect to a database, write SQL queries, manage MDL projects, and operate the MCP server. Use when a user wants to perform any ongoing Wren task after initial setup. Trigger for: write SQL, query data, update MDL, add a model, change connection, rebuild project, restart MCP server.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.0"
---

# Wren Engine — Usage Guide

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-usage` key with this skill's version (`1.0`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-usage** skill is available (remote: X.Y, installed: 1.0).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-usage
> ```

Then continue with the workflow below regardless of update status.

---

This skill is your day-to-day reference for working with Wren Engine. It delegates to focused sub-skills for each task.

---

## Step 0 — Install dependent skills (first time only)

Check whether the required skills are already installed in `~/.claude/skills/`. If any are missing, tell the user to run:

```bash
# Install wren-usage and all its dependencies in one command:
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- wren-usage
```

This installs `wren-usage`, `wren-connection-info`, `generate-mdl`, `wren-project`, `wren-sql`, and `wren-mcp-setup` into `~/.claude/skills/`.

After installation, the user must **start a new session** for the new skills to be loaded.

> If the user only wants the MCP server set up (no Docker yet), use `/wren-quickstart` for a guided end-to-end walkthrough instead.

---

## What do you want to do?

Identify the user's intent and delegate to the appropriate skill:

| Task | Skill |
|------|-------|
| Write or debug a SQL query | `@wren-sql` |
| Connect to a new database / change credentials | `@wren-connection-info` |
| Generate MDL from an existing database | `@generate-mdl` |
| Save MDL to YAML files (version control) | `@wren-project` |
| Load a saved YAML project / rebuild `target/mdl.json` | `@wren-project` |
| Add a new model or column to the MDL | `@wren-project` |
| Start, reset, or reconfigure the MCP server | `@wren-mcp-setup` |
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

Invoke `@wren-connection-info` to:
- Change the data source type or credentials
- Produce a new `connection.yml` + `target/connection.json`
- Switch between `connectionFilePath` (secure) and inline dict

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
2. Invoke `@generate-mdl` — re-introspect the database and rebuild the MDL JSON
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
