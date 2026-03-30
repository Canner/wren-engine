---
name: wren-usage
description: "Wren Engine CLI workflow guide for AI agents. Teaches agents how to answer data questions end-to-end: gather schema context, recall past queries, write SQL through the MDL semantic layer, execute, and learn from results. Covers error recovery, MDL change handling, and data source onboarding. Use when: agent needs to query data, connect a new data source, or handle MDL changes via the wren CLI."
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.0"
---

# Wren Engine CLI — Agent Workflow Guide

This skill teaches you how to use the `wren` CLI to answer data questions. It is decision-driven: it tells you **when** and **why** to run each command, not just **how**.

For memory-specific decisions (when to store, when to recall), see `@wren-memory`.

---

## Core concept: the MDL semantic layer

The `wren` CLI does not query database tables directly. All SQL goes through an **MDL (Model Definition Language)** layer that defines models, columns, relationships, and views. You query model names, not table names.

Two files drive everything:
- `~/.wren/mdl.json` — the semantic model (what tables/columns exist and how they relate)
- `~/.wren/connection_info.json` — how to connect to the database

If both exist, every `wren` command auto-discovers them. No flags needed.

---

## Workflow 1: Answering a data question

This is the most common workflow. A user asks a natural language question and expects data back.

```
User: "What were the top 5 customers by revenue last quarter?"
```

### Step 1 — Gather context

Before writing SQL, you need to know what models and columns are available.

**Decision: which context command?**

| Situation | Command | Why |
|-----------|---------|-----|
| First question in session | `wren memory context -q "<question>"` | Auto-selects full text or search based on schema size |
| You already have schema context from a recent call | Skip | Don't re-fetch what you already know |
| Need a specific model's columns | `wren memory search -q "..." --model orders` | Targeted search |
| Memory extras not installed | `wren dry-plan --sql "SELECT 1"` and read the error for model hints | Fallback — no embedding needed |

### Step 2 — Check for similar past queries

```bash
wren memory recall -q "top customers by revenue" --limit 3
```

If results come back, use them as **few-shot examples** — adapt the SQL pattern to the current question rather than writing from scratch. This significantly improves SQL quality for complex queries.

If no results, proceed without examples.

### Step 3 — Write and execute SQL

Write SQL targeting MDL model names (not raw database tables).

```bash
wren --sql 'SELECT c_name, SUM(o_totalprice) AS revenue
FROM orders JOIN customer ON orders.o_custkey = customer.c_custkey
WHERE o_orderdate >= DATE '\''2025-10-01'\''
GROUP BY 1 ORDER BY 2 DESC LIMIT 5'
```

**Key SQL rules:**
- Use model names from the MDL, not database table names
- Use `CAST(x AS type)` for conversions, not `::type`
- Avoid correlated subqueries — use JOINs or CTEs
- Wren SQL is dialect-neutral (ANSI-ish) — the engine translates to the target dialect

### Step 4 — Handle the result

**Decision: what to do after execution?**

| Outcome | Action |
|---------|--------|
| Success + user confirms result is correct | `wren memory store --nl "..." --sql "..."` |
| Success + user continues with follow-up question | `wren memory store --nl "..." --sql "..."` then handle follow-up |
| Success + user says nothing | Do NOT store — silence is not confirmation |
| Success + user says result is wrong | Do NOT store — fix the SQL |
| Query error | Go to **Error recovery** below |

---

## Workflow 2: Error recovery

When a query fails, diagnose before retrying.

### "table not found" or "model not found"

```
1. Check: is the model name correct?
   → wren memory search -q "<model name>" --type model
   → Compare with what the user asked for

2. Is the MDL deployed?
   → ls ~/.wren/mdl.json
   → If missing: ask user for MDL file or generate one

3. Is the column name correct?
   → wren memory search -q "<column>" --model <model_name>
```

### "connection refused" or "could not connect"

```
1. Check connection file exists:
   → cat ~/.wren/connection_info.json

2. Verify datasource field matches the connector:
   → Must be one of: postgres, mysql, bigquery, snowflake, clickhouse,
     trino, mssql, databricks, redshift, spark, athena, oracle, duckdb

3. Test with a minimal query:
   → wren --sql "SELECT 1"
```

### SQL syntax / planning error

```
1. Use dry-plan to isolate planning vs execution:
   → wren dry-plan --sql "<the failing SQL>"
   → If dry-plan fails: the SQL has MDL-level issues (bad model/column names, unsupported syntax)
   → If dry-plan succeeds but execution fails: the SQL is valid but the DB rejects the translated query

2. Common fixes:
   - Replace :: casts with CAST()
   - Replace correlated subqueries with JOINs
   - Check column types — don't compare varchar to integer without CAST
```

---

## Workflow 3: Connecting a new data source

When the user wants to query a new database for the first time.

### Step 1 — Create connection_info.json

Ask the user for: datasource type, host, port, database, credentials.

Write to `~/.wren/connection_info.json`:

```json
{
  "datasource": "postgres",
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "user": "postgres",
  "password": "..."
}
```

See `wren/docs/connections.md` for all connector formats (BigQuery needs base64-encoded credentials, Snowflake needs account identifier, etc.).

### Step 2 — Test connectivity

```bash
wren --sql "SELECT 1"
```

If this fails, fix connection_info before proceeding.

### Step 3 — Get or create MDL

**Decision: does the user already have an MDL?**

| Situation | Action |
|-----------|--------|
| User has an existing mdl.json | Copy to `~/.wren/mdl.json` |
| User has a YAML project | Build it: the project tooling produces `target/mdl.json` |
| Starting from scratch | The user needs to create an MDL — this defines which tables and columns are queryable. Help them write one based on their database schema. |

### Step 4 — Index schema and verify

```bash
wren memory index
wren --sql "SELECT * FROM <some_model> LIMIT 5"
```

---

## Workflow 4: MDL changes

When the MDL is updated (new model, new column, changed relationship), downstream state becomes stale.

### What needs to happen after MDL changes

```
1. Deploy: copy updated mdl.json to ~/.wren/mdl.json
2. Re-index: wren memory index
   → Schema memory becomes stale if you skip this
3. Verify: wren --sql "SELECT * FROM <changed_model> LIMIT 1"
```

### How to detect stale schema memory

If `wren memory context` returns results that reference models/columns that no longer exist, the index is stale. Re-index.

---

## Command decision tree

When unsure which command to use:

```
Want to get data back?
  → wren --sql "..." (or wren query --sql "...")

Want to see the translated SQL without executing?
  → wren dry-plan --sql "..."

Want to check if SQL is valid against the live DB?
  → wren dry-run --sql "..."

Want schema context before writing SQL?
  → wren memory context -q "..."

Want to find a specific model or column?
  → wren memory search -q "..." [--type model|column]

Want to store a confirmed query?
  → wren memory store --nl "..." --sql "..."

Want few-shot examples for a new question?
  → wren memory recall -q "..."
```

---

## Things to avoid

- **Do not guess model or column names.** Always check schema context first.
- **Do not store queries the user hasn't confirmed.** Query success != correctness.
- **Do not re-index before every query.** Index once per MDL change.
- **Do not use database-specific syntax in wren SQL.** Write ANSI SQL and let the engine translate.
- **Do not pass connection passwords in --connection-info on the command line** if the shell history is shared. Use --connection-file instead.
