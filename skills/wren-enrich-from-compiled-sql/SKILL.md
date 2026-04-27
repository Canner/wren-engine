---
name: wren-enrich-from-compiled-sql
description: "Enrich a dbt-imported Wren project by reading dbt compiled SQL and writing semantic metadata back into model YAML files. Use when the user mentions dbt compiled SQL, missing descriptions, derived columns, lineage, data scope, or asks to enrich a Wren dbt import after `wren context import dbt`."
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.0"
---

# Enrich Wren Metadata from dbt Compiled SQL

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-enrich-from-compiled-sql` key with this skill's version
(from the frontmatter above). If the remote version is newer, notify the user
before proceeding:

> A newer version of the **wren-enrich-from-compiled-sql** skill is available.
> Update with:
> ```
> npx skills add Canner/wren-engine --skill wren-enrich-from-compiled-sql
> ```
> The CLI auto-detects your installed agent. To target a specific one, add
> `--agent <name>` (e.g., `claude-code`, `cursor`, `windsurf`, `cline`).

Then continue with the workflow below regardless of update status.

---

This skill deepens a dbt-imported Wren project by mining `target/compiled/*.sql`
and writing back concise semantic hints:

- `column.properties.derived_from`
- auto-filled `column.properties.description` when docs are missing
- `model.properties.data_scope` from the final `WHERE` clause

It is designed to run **after** `wren context import dbt` has already created a
buildable Wren project. The goal is not to copy compiled SQL into YAML files;
the goal is to translate compiled SQL into compact metadata that helps Wren and
AI agents reason about the data model.

For query workflows after enrichment, see the **wren-usage** skill.

## Prerequisites

- A Wren project generated from dbt import
- `wren_project.yml` contains a `dbt.project_dir` binding, or the dbt project
  path is otherwise known
- dbt compiled SQL exists under `target/compiled/`
- The agent can edit YAML files in the project and run:
  - `wren context validate`
  - `wren context build`

Optional but recommended:

- `sqlglot` available in the environment for expression parsing
- `wren[memory]` installed if the user wants to re-index memory afterward

## Guardrails

These rules are critical:

1. **Do not overwrite good human-authored descriptions by default.**
   Only fill descriptions that are empty, missing, placeholder-like, or if the
   user explicitly asks for replacement.

2. **Do not paste full compiled SQL into YAML.**
   Store concise expressions such as `amount - COALESCE(discount, 0)`, not an
   entire `SELECT` statement.

3. **Only write `derived_from` when the alias-to-expression mapping is clear.**
   If the compiled SQL is too complex to explain confidently, skip
   `derived_from` rather than guessing.

4. **Only write `data_scope` from the final outermost `WHERE` clause.**
   Do not infer scope from join predicates, CTE filters that may be rewritten,
   or assumptions about upstream staging logic unless they appear in the final
   compiled query.

5. **Keep descriptions short and plain-English.**
   A one-sentence explanation is usually enough.

6. **Validate and rebuild before claiming success.**
   If the enrichment breaks the project, fix or revert the problematic metadata.

## Phase 0 — Detect the project and dbt binding

1. Confirm the current directory is a Wren project by locating
   `wren_project.yml`.
2. Read `wren_project.yml` and confirm the project has a `dbt` section.
3. Resolve the dbt project path:
   - Prefer `dbt.project_dir` from `wren_project.yml`
   - If it is relative, resolve it relative to the Wren project root
4. Confirm the compiled directory exists:

```text
<dbt-project>/target/compiled/
```

If compiled SQL is missing, instruct the user to run:

```bash
dbt compile
```

If only `manifest.json` and `catalog.json` exist but no compiled SQL, stop and
explain that this skill specifically needs `target/compiled/*.sql`.

## Phase 1 — Choose scope

Decide what to enrich:

- **Single model** when the user mentions one model or when validating the
  workflow on a sample first
- **All imported dbt models** when the user asks for a full enrichment pass

Skip:

- models with no matching compiled SQL file
- raw `source` imports that are direct table references
- any model whose metadata file is missing

Match compiled SQL files to models by dbt model name / alias. Typical paths
look like:

```text
target/compiled/<project-name>/models/.../<model-name>.sql
```

## Phase 2 — Extract model-level data scope

For each matched model:

1. Parse the outermost `SELECT`.
2. Locate the final `WHERE` clause on that outermost query.
3. If present, normalize it into a single compact SQL string and write:

```yaml
properties:
  data_scope: "status != 'cancelled' AND amount > 0"
```

4. If there is no outermost `WHERE`, leave `data_scope` unset.

Good examples:

- `status != 'cancelled'`
- `amount > 0`
- `email NOT LIKE '%@test.com'`

Bad examples:

- copying the entire query
- including `JOIN ... ON ...` predicates in `data_scope`
- paraphrasing filters that do not appear in compiled SQL

## Phase 3 — Extract column derivations

Work from the final `SELECT` list of the compiled SQL.

For each output column:

1. Identify the alias / output column name.
2. Find the expression that produced it.
3. If the expression is not a trivial passthrough, write:

```yaml
columns:
  - name: net_amount
    properties:
      derived_from: "amount - COALESCE(discount, 0)"
```

Use these heuristics:

- `id AS order_id` → treat as rename; `derived_from` is optional
- direct passthrough `status` → usually skip `derived_from`
- arithmetic / boolean / date / case / aggregate / window expressions
  → usually write `derived_from`
- keep the expression concise and preserve SQL syntax

Good examples:

- `amount - COALESCE(discount, 0)`
- `DATE_TRUNC('month', created_at)`
- `SUM(net_amount) OVER (PARTITION BY customer_id)`
- `CASE WHEN is_deleted THEN 'deleted' ELSE status END`

## Phase 4 — Fill missing descriptions

Only fill descriptions when they are empty or clearly auto-generated noise.

Use the expression and column name to generate a short description:

- `net_amount` + `amount - COALESCE(discount, 0)`
  → `Order amount after subtracting discount`
- `order_month` + `DATE_TRUNC('month', created_at)`
  → `Order creation month`
- `lifetime_value` + `SUM(net_amount) OVER (PARTITION BY customer_id)`
  → `Running customer revenue total`

Description rules:

- one sentence
- no markdown tables
- no speculation about business meaning unless the expression supports it
- prefer business-facing language over SQL jargon

If an existing description is already present and reasonable, keep it.

## Phase 5 — Write YAML safely

Edit the target `models/*/metadata.yml` files directly.

For each affected model:

- add `properties.data_scope` only when extracted confidently
- add `column.properties.derived_from` only when extracted confidently
- add `column.properties.description` only when missing

Do not:

- reorder the whole file unnecessarily
- remove existing dbt test metadata
- rewrite unrelated fields

If the model already has a `properties` block, merge into it instead of
replacing it.

## Phase 6 — Validate, build, and optionally re-index

After editing:

```bash
wren context validate --path <project-dir>
wren context build --path <project-dir>
```

If the user uses memory, also run:

```bash
wren memory index --path <project-dir>
```

Only report success after validation and build pass.

## Recommended workflow summary

Use this exact order:

1. Detect Wren project and dbt compiled SQL
2. Pick model scope
3. Parse final compiled SQL
4. Update `derived_from`
5. Fill missing descriptions
6. Update `data_scope`
7. Validate and build
8. Re-index memory if requested

## Example outcome

Before:

```yaml
columns:
  - name: net_amount
    type: NUMERIC
properties:
  description: Cleaned orders
```

After:

```yaml
columns:
  - name: net_amount
    type: NUMERIC
    properties:
      description: Order amount after subtracting discount
      derived_from: "amount - COALESCE(discount, 0)"
properties:
  description: Cleaned orders
  data_scope: "status != 'cancelled' AND amount > 0"
```

## When to stop and ask the user

Pause and ask only if:

- multiple dbt projects could match the current Wren project
- compiled SQL is missing for the requested model set
- the user has existing descriptions that would need to be overwritten to
  continue
- the SQL is too ambiguous to infer safe metadata

Otherwise, proceed end-to-end and present the enriched files plus the
validation/build result.
