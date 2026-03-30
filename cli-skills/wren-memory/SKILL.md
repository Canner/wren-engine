---
name: wren-memory
description: "Schema and query memory for Wren Engine — index MDL schema for semantic search, store and recall NL-to-SQL pairs for few-shot retrieval. Guides the agent on when to index, search, store, and recall using the wren memory CLI. Use when: the agent needs schema context before writing SQL, wants to find relevant models/columns by natural language, or should store a confirmed-correct query for future reference."
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.0"
---

# Wren Memory

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-memory` key with this skill's version (from the frontmatter above).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-memory** skill is available.
> Update with:
> ```
> npx skills add Canner/wren-engine --skill wren-memory --agent claude-code
> ```

Then continue with the workflow below regardless of update status.

---

## What is Wren Memory?

Wren Memory is a LanceDB-backed local memory layer for the `wren` CLI. It provides two capabilities:

1. **Schema Memory** — index MDL models, columns, relationships, and views for semantic search
2. **Query Memory** — store confirmed NL-to-SQL pairs for few-shot retrieval

Requires the `memory` extra: `pip install 'wren-engine[memory]'`

---

## When to use each command

### Schema context: `context` > `describe` > `search`

Before writing SQL, you need schema context. Choose the right command:

| Command | When to use |
|---------|-------------|
| `wren memory context -q "..."` | **Default choice.** Auto-selects full text or embedding search based on schema size. |
| `wren memory describe` | When you want the full schema and know it is small. |
| `wren memory search -q "..."` | When you need fine-grained filtering (by item type or model name). |

**Workflow — before writing SQL:**

```
1. User asks a question in natural language
2. Run: wren memory context -q "<user's question>" --mdl ~/.wren/mdl.json
3. Use the returned schema context to write SQL
4. If context strategy is "search" and results are insufficient,
   try a broader query or different keywords
```

### Indexing: `index`

Index the MDL schema so that `search` and `context` (search strategy) work.

**When to index:**
- After deploying a new or updated MDL (`wren memory index`)
- When `wren memory status` shows `schema_items: 0 rows`
- When the MDL has changed and the schema memory is stale

**When NOT to index:**
- Before every query — indexing is expensive, do it once per MDL change
- When only using `describe` or `context` with full strategy — those read the MDL directly

```bash
wren memory index --mdl ~/.wren/mdl.json
```

### Storing queries: `store`

Store a NL-to-SQL pair so it can be recalled later as a few-shot example.

**When to store — ALL of these must be true:**
1. The SQL query executed successfully
2. The user confirmed the result is correct, OR the user continued working with the result (asked a follow-up question, exported the data, etc.)
3. There is a clear natural language question that the SQL answers

**When NOT to store:**
- The query failed or returned an error
- The user said the result is wrong or asked to fix it
- The query is exploratory / throwaway (e.g. `SELECT * FROM orders LIMIT 5`)
- There is no natural language question — just raw SQL from the user
- The user explicitly asked not to store it

```bash
wren memory store \
  --nl "top 5 customers by revenue last quarter" \
  --sql "SELECT c_name, SUM(o_totalprice) AS revenue FROM orders JOIN customer ON orders.o_custkey = customer.c_custkey WHERE o_orderdate >= DATE '2025-10-01' GROUP BY 1 ORDER BY 2 DESC LIMIT 5" \
  --datasource postgres
```

**Important:** The `--nl` value should be the user's original question, not a paraphrase. This ensures the embedding matches future similar questions.

### Recalling queries: `recall`

Search past NL-to-SQL pairs for few-shot examples before writing new SQL.

**When to recall:**
- Before writing SQL for a new question, especially if it is complex
- When the user asks something similar to a past question
- When you want few-shot examples to improve SQL quality

**Workflow — SQL generation with memory:**

```
1. User asks: "show me monthly revenue trend for the top 3 product categories"
2. Run: wren memory recall -q "monthly revenue trend by product category" --limit 3
3. Run: wren memory context -q "revenue product category monthly" --mdl ~/.wren/mdl.json
4. Use recalled queries as few-shot examples + schema context to write SQL
5. Execute the SQL
6. If user confirms result → store the new pair
```

### Housekeeping: `status` and `reset`

```bash
wren memory status              # check what is indexed
wren memory reset --force       # clear everything and start fresh
```

---

## Complete workflow example

Here is the full lifecycle for a typical agent session:

```
Session start:
  1. Check: wren memory status
     → If schema_items is 0 or MDL has changed: wren memory index

User asks a question:
  2. Recall: wren memory recall -q "<question>" --limit 3
     → Use any results as few-shot examples
  3. Context: wren memory context -q "<question>"
     → Get relevant schema information
  4. Write SQL using recalled examples + schema context
  5. Execute: wren query --sql "..."

After successful execution:
  6. Show results to user
  7. Wait for user's response:
     - User confirms / continues → wren memory store --nl "..." --sql "..."
     - User says "wrong" / asks to fix → do NOT store, fix the SQL instead
     - User says nothing and moves on → do NOT store (silence ≠ confirmation)
```

---

## CLI quick reference

| Command | Purpose |
|---------|---------|
| `wren memory index [--mdl PATH]` | Index MDL schema into LanceDB |
| `wren memory describe [--mdl PATH]` | Print full schema as plain text |
| `wren memory context -q "..." [--mdl PATH]` | Get schema context (auto full/search) |
| `wren memory search -q "..." [--type T] [--model M]` | Semantic search over schema |
| `wren memory store --nl "..." --sql "..."` | Store a confirmed NL-to-SQL pair |
| `wren memory recall -q "..." [--limit N]` | Recall similar past queries |
| `wren memory status` | Show index statistics |
| `wren memory reset [--force]` | Drop all memory tables |

All commands accept `--path DIR` to override the default storage location (`~/.wren/memory/`).
