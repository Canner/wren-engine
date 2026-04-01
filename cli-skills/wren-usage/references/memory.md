# Wren Memory — When to index, context, store, and recall

This reference covers the decision logic for each memory command. The main workflow is in the parent SKILL.md.

---

## Schema context: `fetch` and `describe`

| Command | When to use |
|---------|-------------|
| `wren memory fetch -q "..."` | Default. Auto-selects full text (small schema) or embedding search (large schema) based on a 30K-char threshold. |
| `wren memory fetch -q "..." --type T --model M` | When you need filtering (forces search strategy on large schemas). |
| `wren memory describe` | When you want the full schema text and know it is small. |

The hybrid strategy works like this:
- Below 30K characters (~8K tokens): returns the entire schema as structured plain text — the LLM sees complete model-to-column relationships, join paths, and primary keys
- Above 30K characters: returns embedding search results — only the most relevant fragments

CJK-heavy schemas switch to search sooner (~1.5 chars per token vs 4 for English), which is the safe direction.

Override with `--threshold`:
```bash
wren memory fetch -q "revenue" --threshold 50000   # raise for larger context windows
```

---

## Indexing: `wren memory index`

**When to index:**
- After deploying a new or updated MDL
- When `wren memory status` shows `schema_items: 0 rows`
- When `wren memory fetch` returns stale results (references deleted models)

**When NOT to index:**
- Before every query — indexing is expensive, do it once per MDL change
- When only using `describe` or `fetch` with full strategy — those read the MDL directly

```bash
wren memory index --mdl ~/.wren/mdl.json
```

---

## Storing queries: `wren memory store`

**Store when ALL of these are true:**
1. The SQL query executed successfully
2. The user confirmed the result is correct, OR continued working with it (follow-up question, exported data, etc.)
3. There is a clear natural language question that the SQL answers

**Do NOT store when:**
- The query failed or returned an error
- The user said the result is wrong or asked to fix it
- The query is exploratory / throwaway (`SELECT * FROM orders LIMIT 5`)
- There is no natural language question — just raw SQL
- The user explicitly asked not to store it

```bash
wren memory store \
  --nl "top 5 customers by revenue last quarter" \
  --sql "SELECT c_name, SUM(o_totalprice) AS revenue ..." \
  --datasource postgres
```

The `--nl` value should be the user's original question, not a paraphrase.

---

## Recalling queries: `wren memory recall`

**When to recall:**
- Before writing SQL for a new question, especially complex ones
- When the user asks something similar to a past question

```bash
wren memory recall -q "monthly revenue by category" --limit 3
```

Use results as few-shot examples: adapt the SQL pattern to the current question.

---

## Full lifecycle example

```
Session start:
  1. wren memory status → if schema_items is 0: wren memory index

User asks a question:
  2. wren memory recall -q "<question>" --limit 3
  3. wren memory fetch -q "<question>"
  4. Write SQL using recalled examples + schema context
  5. wren --sql "..."

After execution:
  6. Show results to user
  7. User confirms → wren memory store --nl "..." --sql "..."
     User says wrong → fix SQL, do NOT store
     User silent → do NOT store
```

---

## Housekeeping

```bash
wren memory status              # path, table names, row counts
wren memory reset --force       # drop everything, start fresh
```

All memory commands accept `--path DIR` to override `~/.wren/memory/`.
