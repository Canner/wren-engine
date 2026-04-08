# Skills

Wren Engine provides **skills** — reusable AI agent workflow guides that teach Claude Code (or other AI coding agents) how to use the Wren CLI effectively. Skills are not plugins or extensions; they are structured prompts with decision trees that guide an agent through multi-step tasks.

## Available skills

| Skill | Purpose |
|-------|---------|
| **wren-usage** | Day-to-day workflow: gather schema context, recall past queries, write SQL, execute, store results |
| **wren-generate-mdl** | One-time setup: explore database schema, normalize types, scaffold MDL YAML project |

## Installation

```bash
# All skills at once
npx skills add Canner/wren-engine --skill '*' --agent claude-code

# Or via install script
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash
```

After installation, **start a new Claude Code session** — skills are loaded at session start.

### Update skills

Skills check for updates automatically and notify the agent when a newer version is available. To force-update:

```bash
# All skills
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force

# Single skill
curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-generate-mdl
```

---

## wren-usage

The primary skill for day-to-day querying. It guides the agent through a complete query lifecycle.

### Query workflow

```
User asks a question
  │
  ├── 1. Gather context
  │     wren memory fetch -q "..."
  │     wren context instructions      (first query only)
  │
  ├── 2. Recall past queries
  │     wren memory recall -q "..." --limit 3
  │
  ├── 3. Assess complexity
  │     Simple → write SQL directly
  │     Complex → decompose into sub-questions
  │
  ├── 4. Write and execute SQL
  │     Simple: wren --sql "..."
  │     Complex: wren dry-plan first, then execute
  │
  └── 5. Store result
        wren memory store --nl "..." --sql "..."
```

### Error recovery

The skill includes a two-layer error diagnosis strategy:

| Layer | Tool | Diagnoses |
|-------|------|-----------|
| **MDL-level** | `wren dry-plan` fails | Wrong model/column names, missing relationships |
| **DB-level** | `wren dry-plan` succeeds but execution fails | Type mismatch, permissions, dialect issues |

The agent checks `dry-plan` output first to isolate whether the error is in the semantic layer or the database.

### Additional workflows

| Workflow | When |
|----------|------|
| **Connect new data source** | `wren profile add` → `wren context init` → build → index |
| **After MDL changes** | `wren context validate` → `wren context build` → `wren memory index` |

### Reference files

The skill includes two reference documents loaded on demand:

- **memory.md** — Decision logic for when to `index`, `fetch`, `store`, and `recall`. Covers the hybrid retrieval strategy, store-by-default policy, and full lifecycle examples.
- **wren-sql.md** — How the CTE-based rewrite pipeline works. Explains how the engine injects model CTEs, what SQL features are supported, and how to use `dry-plan` to diagnose errors layer by layer.

---

## wren-generate-mdl

A one-time setup skill that walks the agent through creating an MDL project from a live database.

### Seven-phase workflow

| Phase | Goal | Key actions |
|-------|------|-------------|
| **1. Connect** | Confirm database access | Test connection via SQLAlchemy, driver, or `wren profile debug` |
| **2. Discover** | Collect schema metadata | Introspect tables, columns, types, foreign keys |
| **3. Normalize** | Convert types | `wren utils parse-type` or Python `parse_type()` |
| **4. Scaffold** | Write YAML project | `wren context init`, create model files, relationships |
| **5. Validate** | Check integrity | `wren context validate` → `wren context build` |
| **6. Index** | Initialize memory | `wren memory index` |
| **7. Iterate** | Refine with user | Add descriptions, calculated columns, views |

### Schema discovery methods

The skill is tool-agnostic — it uses whatever database access the agent has:

| Method | Best for |
|--------|----------|
| **SQLAlchemy** `inspect()` | Most databases — richest metadata (PKs, FKs, types) |
| **Database driver** | When SQLAlchemy is unavailable — query `information_schema` directly |
| **Raw SQL via wren** | Bootstrapping when no Python driver is installed |

### Type normalization

Raw database types must be normalized before use in MDL:

```bash
# Single type
wren utils parse-type --type "character varying(255)" --dialect postgres
# → VARCHAR(255)

# Batch (stdin JSON)
echo '[{"column":"id","raw_type":"int8"}]' | wren utils parse-types --dialect postgres
```

Or via Python:

```python
from wren.type_mapping import parse_type
normalized = parse_type("character varying(255)", "postgres")  # → "VARCHAR(255)"
```

---

## Skill structure

Skills are installed to `~/.claude/skills/` with this layout:

```
~/.claude/skills/
├── wren-usage/
│   ├── SKILL.md              # Main workflow instructions
│   └── references/
│       ├── memory.md          # Memory command decision logic
│       └── wren-sql.md        # CTE rewrite pipeline reference
└── wren-generate-mdl/
    └── SKILL.md              # MDL generation workflow
```

Each `SKILL.md` has YAML frontmatter with name, description, version, and license. The agent loads the main SKILL.md when triggered, and loads reference files on demand when deeper context is needed.
