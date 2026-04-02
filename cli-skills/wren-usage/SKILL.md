---
name: wren-usage
description: "Wren Engine CLI workflow guide for AI agents. Answer data questions end-to-end using the wren CLI: gather schema context, recall past queries, write SQL through the MDL semantic layer, execute, and learn from confirmed results. Use when: agent needs to query data, connect a data source, handle errors, or manage MDL changes via the wren CLI."
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.0"
---

# Wren Engine CLI — Agent Workflow Guide

The `wren` CLI queries databases through an MDL (Model Definition Language) semantic layer. You write SQL against model names, not raw tables. The engine translates to the target dialect.

Two files drive everything (auto-discovered from `~/.wren/`):
- `mdl.json` — the semantic model
- `connection_info.json` — database credentials + `datasource` field (e.g. `"datasource": "postgres"`)

The data source is always read from `connection_info.json`. There is no `--datasource` flag on execution commands (`query`, `dry-run`, `validate`). Only `dry-plan` accepts `--datasource` / `-d` as an override (for transpile-only use without a connection file).

For memory-specific decisions, see [references/memory.md](references/memory.md).
For SQL syntax, CTE-based modeling, and error diagnosis, see [references/wren-sql.md](references/wren-sql.md).

---

## Workflow 1: Answering a data question

### Step 1 — Gather context

| Situation | Command |
|-----------|---------|
| Default | `wren memory fetch -q "<question>"` |
| Need specific model's columns | `wren memory fetch -q "..." --model <name> --threshold 0` |
| Memory not installed | Read `~/.wren/mdl.json` directly |

### Step 2 — Recall past queries

```bash
wren memory recall -q "<question>" --limit 3
```

Use results as few-shot examples. Skip if empty.

### Step 3 — Write and execute SQL

```bash
wren --sql 'SELECT c_name, SUM(o_totalprice) FROM orders
JOIN customer ON orders.o_custkey = customer.c_custkey
GROUP BY 1 ORDER BY 2 DESC LIMIT 5'
```

**SQL rules:**
- Target MDL model names, not database tables
- Write dialect-neutral SQL — the engine translates

### Step 4 — Handle the result

| Outcome | Action |
|---------|--------|
| User confirms correct | `wren memory store --nl "..." --sql "..."` |
| User continues with follow-up | Store, then handle follow-up |
| User says nothing | Do NOT store |
| User says wrong | Do NOT store — fix the SQL |
| Query error | See Error recovery below |

---

## Workflow 2: Error recovery

### "table not found"

1. Verify model name: `wren memory fetch -q "<name>" --type model --threshold 0`
2. Check MDL exists: `ls ~/.wren/mdl.json`
3. Verify column: `wren memory fetch -q "<column>" --model <name> --threshold 0`

### Connection error

1. Check: `cat ~/.wren/connection_info.json`
2. Verify the `datasource` field is present and valid
3. Test: `wren --sql "SELECT 1"`
4. Valid datasource values: `postgres`, `mysql`, `bigquery`, `snowflake`, `clickhouse`, `trino`, `mssql`, `databricks`, `redshift`, `spark`, `athena`, `oracle`, `duckdb`
5. Both flat format (`{"datasource": ..., "host": ...}`) and MCP envelope format (`{"datasource": ..., "properties": {...}}`) are accepted

### SQL syntax / planning error

1. Isolate the layer:
   - `wren dry-plan --sql "..."` — if this fails, it is an MDL-level issue
   - If dry-plan succeeds but execution fails, the DB rejects the translated SQL
2. Compare dry-plan output with the DB error message — see [references/wren-sql.md](references/wren-sql.md) for the CTE rewrite pipeline and common error patterns

---

## Workflow 3: Connecting a new data source

1. Create `~/.wren/connection_info.json` — see [wren/docs/connections.md](../../wren/docs/connections.md) for per-connector formats
2. Test: `wren --sql "SELECT 1"`
3. Place or create `~/.wren/mdl.json`
4. Index: `wren memory index`
5. Verify: `wren --sql "SELECT * FROM <model> LIMIT 5"`

---

## Workflow 4: After MDL changes

When the MDL is updated, downstream state goes stale:

```bash
# 1. Deploy updated MDL
cp updated-mdl.json ~/.wren/mdl.json

# 2. Re-index schema memory
wren memory index

# 3. Verify
wren --sql "SELECT * FROM <changed_model> LIMIT 1"
```

---

## Command decision tree

```
Get data back           → wren --sql "..."
See translated SQL only → wren dry-plan --sql "..." (accepts -d <datasource> if no connection file)
Validate against DB     → wren dry-run --sql "..."
Schema context          → wren memory fetch -q "..."
Filter by type/model    → wren memory fetch -q "..." --type T --model M --threshold 0
Store confirmed query   → wren memory store --nl "..." --sql "..."
Few-shot examples       → wren memory recall -q "..."
Index stats             → wren memory status
Re-index after MDL change → wren memory index
```

---

## Things to avoid

- Do not guess model or column names — check context first
- Do not store queries the user has not confirmed — success != correctness
- Do not re-index before every query — once per MDL change
- Do not pass passwords via `--connection-info` if shell history is shared — use `--connection-file`
