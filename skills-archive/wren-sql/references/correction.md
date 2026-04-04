# SQL Error Diagnosis and Correction

Wren Engine processes SQL through multiple stages. Errors include a `phase` field and sometimes a `dialectSql` field showing which SQL layer failed.

## Pipeline Stages

1. **Parsing** — Wren SQL → AST
2. **Planning** — AST → IR (subqueries generated per model definition)
3. **Unparsing** — IR → generic SQL (DataFusion dialect)
4. **Transpile** — generic SQL → target database dialect
5. **Execution** — transpiled SQL runs against the target database

**Three SQL layers:**
- **Wren SQL** — user-submitted SQL (only modify this one)
- **Planned SQL** — after planning/unparsing
- **Dialect SQL** — after transpiling, database-specific

**Example error response:**
```json
{
  "phase": "SQL_EXECUTION",
  "metadata": { "dialectSql": "SELECT CURRENT_TIMESTAMP() - INTERVAL '1' MONTH" },
  "message": "TIMESTAMP +/- INTERVAL is not supported for intervals with non-zero MONTH or YEAR part."
}
```

---

## Step 1 — Identify the Error Phase

| Phase | Cause | Fix |
|-------|-------|-----|
| **Parsing error** | Wren SQL has syntax issues | Fix SQL syntax |
| **Planning error** | Unsupported construct or missing model/column | Verify names match MDL exactly |
| **Transpiling error** | Internal bug (`SQLGLOT_ERROR`, `GENERIC_INTERNAL_ERROR`) | Report to Wren support |
| **Execution error** | Data incompatibility with target DB | See below |

**Execution error sub-cases:**
- Not-found in `__source` (deepest subquery) → model definition out of sync with DB schema → **user error**, not a bug
- Not-found in main/joined query → **internal error**, report to Wren support
- Error during execution but not dry-run → data issue (NULLs, type mismatches, runtime constraints)
- Timeout / resource limit → `EXTERNAL ERROR`; simplify query or check DB performance

---

## Step 2 — Modify the Wren SQL

- Only modify the **Wren SQL**. Never modify Planned SQL or Dialect SQL directly.
- Think about what change to the Wren SQL would produce the correct Planned/Dialect SQL.
- Use alternative constructs if direct SQL triggers a known unsupported feature.

---

## Step 3 — Test

Resubmit the modified Wren SQL. Repeat until execution succeeds.

---

## Step 4 — Validate Results

- Always dry-run first, then execute.
- Verify results meet expected outcomes. Refine if incorrect.

---

## Step 5 — Report Bugs (Last Resort)

First, find an alternative Wren SQL that achieves the same result without triggering the bug.

Only report a bug if no alternative exists AND the SQL is reasonable and should work:
- `GENERIC_INTERNAL_ERROR` or `SQLGLOT_ERROR` → report as **internal error** to Wren support
- Timeout / resource limit with no alternative → report as **external error** to the user
