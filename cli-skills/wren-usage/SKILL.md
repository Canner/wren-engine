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

### Step 2.5 — Assess complexity (before writing SQL)

If the question involves **any** of the following, consider decomposing:
- Multiple metrics or aggregations (e.g., "churn rate AND expansion revenue")
- Multi-step calculations (e.g., "month-over-month growth rate")
- Comparisons across segments (e.g., "by plan tier, by region")
- Time-series analysis requiring baseline + change (e.g., "retention curve")

**Decomposition strategy:**
1. Identify the sub-questions (e.g., "total subscribers at start" + "subscribers who cancelled" → churn rate)
2. For each sub-question:
   - `wren memory recall -q "<sub-question>"` — check if a similar pattern exists
   - Write and execute a simple SQL
   - Note the result
3. Combine sub-results to answer the original question

**When NOT to decompose:**
- Single-table aggregation with GROUP BY — just write the SQL
- Simple JOINs that the MDL relationships already define
- Questions where `memory recall` returns a near-exact match

This is a judgment call, not a rigid rule. If you're confident in a single
query, go ahead. Decompose when the SQL would be hard to debug if it fails.

### Step 3 — Write, verify, and execute SQL

**For simple queries** (single table, straightforward aggregation):
Execute directly:
```bash
wren --sql 'SELECT c_name, SUM(o_totalprice) FROM orders
JOIN customer ON orders.o_custkey = customer.c_custkey
GROUP BY 1 ORDER BY 2 DESC LIMIT 5'
```

**For complex queries** (JOINs, subqueries, multi-step):
Verify first with dry-plan:
```bash
wren dry-plan --sql 'SELECT ...'
```

Check the expanded SQL output:
- Are the correct models and columns referenced?
- Do the JOINs match expected relationships?
- Are CTEs expanded correctly?

If the expanded SQL looks wrong, fix before executing.
If it looks correct, proceed:
```bash
wren --sql 'SELECT ...'
```

**SQL rules:**
- Target MDL model names, not database tables
- Write dialect-neutral SQL — the engine translates

### Step 4 — Store and continue

After successful execution, **store the query by default**:

```bash
wren memory store --nl "<user's original question>" --sql "<the SQL>"
```

**Skip storing only when:**
- The query failed or returned an error
- The user said the result is wrong
- The query is exploratory (`SELECT * ... LIMIT N` without analytical clauses)
- The user explicitly asked not to store

The CLI auto-detects exploratory queries — if you see no store hint
after execution, the query was classified as exploratory.

| Outcome | Action |
|---------|--------|
| User confirms correct | Store |
| User continues with follow-up | Store, then handle follow-up |
| User says nothing (but question had clear NL description) | Store |
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

### SQL syntax / planning error (enhanced)

#### Layer 1: Identify the failure point

```bash
wren dry-plan --sql "<failed SQL>"
```

| dry-plan result | Failure layer | Next step |
|-----------------|---------------|-----------|
| dry-plan fails | MDL / semantic | → Layer 2A |
| dry-plan succeeds, execution fails | DB / dialect | → Layer 2B |

#### Layer 2A: MDL-level diagnosis (dry-plan failed)

The dry-plan error message tells you exactly what's wrong:

| Error pattern | Diagnosis | Fix |
|---------------|-----------|-----|
| `column 'X' not found in model 'Y'` | Wrong column name | `wren memory fetch -q "X" --model Y --threshold 0` to find correct name |
| `model 'X' not found` | Wrong model name | `wren memory fetch -q "X" --type model --threshold 0` |
| `ambiguous column 'X'` | Column exists in multiple models | Qualify with model name: `ModelName.column` |
| Planning error with JOIN | Relationship not defined in MDL | Check available relationships in context |

**Key principle**: Fix ONE issue at a time. Re-run dry-plan after each fix
to see if new errors surface.

#### Layer 2B: DB-level diagnosis (dry-plan OK, execution failed)

The DB error + dry-plan output together pinpoint the issue:

1. Read the dry-plan expanded SQL — this is what actually runs on the DB
2. Compare with the DB error message:

| Error pattern | Diagnosis | Fix |
|---------------|-----------|-----|
| Type mismatch | Column type differs from assumed | Check column type in context, add explicit CAST |
| Function not supported | Dialect-specific function | Use dialect-neutral alternative |
| Permission denied | Table/schema access | Check connection credentials |
| Timeout | Query too expensive | Simplify: reduce JOINs, add filters, LIMIT |

**For small models**: If the error message is unclear, try simplifying
the query to the smallest failing fragment. Execute subqueries independently
to isolate which part fails.

For the CTE rewrite pipeline and additional error patterns, see [references/wren-sql.md](references/wren-sql.md).

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
- Do not store failed queries or queries the user said are wrong
- Do not skip storing successful queries with a clear NL question — default is to store
- Do not re-index before every query — once per MDL change
- Do not pass passwords via `--connection-info` if shell history is shared — use `--connection-file`
