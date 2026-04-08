# Memory

Wren Engine includes a **memory layer** — a LanceDB-backed semantic index that gives AI agents the context they need to write accurate SQL. Instead of sending the entire schema to an LLM on every question, the memory layer provides targeted context: relevant tables, columns, and past query examples.

## Why memory matters

Without memory, an AI agent must either:

- Receive the **full schema** in every prompt — works for small databases but quickly exceeds context limits
- Guess which tables are relevant — leads to hallucinated column names and wrong joins

The memory layer solves both problems by indexing the MDL schema and storing confirmed NL-SQL pairs. At query time it retrieves only what the agent needs.

## What gets indexed

The memory layer manages two collections:

| Collection | Contents | Source | Rebuildable? |
|------------|----------|--------|-------------|
| **schema_items** | Models, columns, relationships, views, instructions | MDL manifest + `instructions.md` | Yes — `wren memory index` |
| **query_history** | Natural-language → SQL pairs | Stored after successful queries | No — built up over time |

Both collections live in `<project>/.wren/memory/` (or `~/.wren/memory/` outside a project).

## Installation

The memory system requires the `memory` extra:

```bash
pip install "wren-engine[memory]"
```

## Indexing the schema

After creating or updating your MDL project, index the schema:

```bash
wren memory index
```

This parses the compiled `target/mdl.json`, generates local embeddings for every schema item, and stores them in LanceDB. Re-index whenever you change models, columns, relationships, or instructions:

```bash
wren context build
wren memory index
```

Check the index status:

```bash
wren memory status
# Path: /Users/you/my-project/.wren/memory
#   schema_items: 47 rows
#   query_history: 12 rows
```

## Fetching schema context

`wren memory fetch` is the primary way agents get schema context. It automatically picks the best retrieval strategy based on schema size:

| Schema size | Strategy | What the agent sees |
|-------------|----------|---------------------|
| Below 30,000 chars (~8K tokens) | **Full text** | Complete schema with all model-column relationships, join paths, and primary keys |
| Above 30,000 chars | **Embedding search** | Top-k most relevant fragments for the query |

```bash
wren memory fetch -q "customer order price"
wren memory fetch -q "revenue" --type column --model orders
wren memory fetch -q "日期" --threshold 50000 --output json
```

### Why hybrid?

Small schemas give better results as full text — the LLM sees the complete structure rather than isolated fragments. Large schemas don't fit in a single prompt, so embedding search retrieves only what's relevant.

The threshold is measured in characters (not tokens) because character counting is free. The 4:1 chars-to-tokens ratio holds for English; CJK text compresses less (~1.5:1), so CJK-heavy schemas switch to search sooner — the conservative direction.

Override with `--threshold`:

```bash
wren memory fetch -q "revenue" --threshold 50000   # raise for larger context windows
```

### Full schema without search

`wren memory describe` prints the entire schema as structured plain text — no embeddings or LanceDB required:

```bash
wren memory describe
```

## Storing and recalling queries

Every successful query can be stored as a natural-language → SQL pair. These pairs serve as **few-shot examples** for future questions — the more you store, the better the agent gets at writing SQL for your domain.

### Storing a query

```bash
wren memory store \
  --nl "top 5 customers by revenue last quarter" \
  --sql "SELECT c.first_name, SUM(o.amount) AS revenue FROM customers c JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_date >= '2024-10-01' GROUP BY 1 ORDER BY 2 DESC LIMIT 5" \
  --datasource duckdb
```

**When to store:**
- Query executed successfully and the result is correct
- There is a clear natural-language question behind the query

**When NOT to store:**
- The query failed or returned wrong results
- The query is exploratory / throwaway (`SELECT * FROM orders LIMIT 5`)
- There is no natural-language question — just raw SQL

### Recalling similar queries

Before writing new SQL, search for similar past queries:

```bash
wren memory recall -q "best customers"
wren memory recall -q "月度營收" --datasource mysql --limit 5 --output json
```

Results are returned ranked by semantic similarity. Use them as few-shot examples — adapt the SQL pattern to the current question.

## Agent workflow

The memory layer fits into the agent's query workflow like this:

```
User asks a question
  │
  ├── 1. wren memory recall -q "..."     → find similar past queries (few-shot examples)
  ├── 2. wren memory fetch -q "..."      → get relevant schema context
  ├── 3. Write SQL using examples + context
  ├── 4. wren --sql "..."                → execute
  │
  └── 5. wren memory store --nl "..." --sql "..."   → save for future recall
```

Each stored query improves future recall accuracy — the system learns from usage.

## Housekeeping

```bash
wren memory status              # show index stats
wren memory reset               # drop all tables (prompts for confirmation)
wren memory reset --force       # drop without confirmation
```

## Storage and version control

Memory files are binary (LanceDB format) and stored in `<project>/.wren/memory/`. By default this directory is gitignored.

- **schema_items** — fully rebuildable from `wren memory index`, safe to delete
- **query_history** — accumulated NL-SQL pairs from usage, **not rebuildable**

If your team wants to share confirmed query history as few-shot examples, you can commit `.wren/memory/` — but be aware that LanceDB files may produce merge conflicts when multiple people store concurrently.
