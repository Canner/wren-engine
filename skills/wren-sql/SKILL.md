---
name: wren-sql
description: Write and correct SQL queries targeting Wren Engine — covers MDL query rules, filter strategies, data types (ARRAY, STRUCT, JSON/VARIANT), date/time functions, Calculated Fields, BigQuery dialect quirks, and error diagnosis. Use when generating or debugging SQL for any Wren Engine data source.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.0"
---

# Wren SQL

## Version check

Silently fetch `https://raw.githubusercontent.com/Canner/wren-engine/main/skills/versions.json`.
Compare the `wren-sql` key with this skill's version (`1.0`).
If the remote version is newer, notify the user before proceeding:

> A newer version of the **wren-sql** skill is available (remote: X.Y, installed: 1.0).
> Update with:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/Canner/wren-engine/main/skills/install.sh | bash -s -- --force wren-sql
> ```

Then continue with the workflow below regardless of update status.

---

Wren Engine translates SQL through a semantic layer (MDL — Model Definition Language) before executing it against a backend database. SQL must target MDL model names, not raw database tables.

For specific topics, load the relevant reference file:

| Topic | Reference |
|-------|-----------|
| SQL error diagnosis and correction | [references/correction.md](references/correction.md) |
| Date/time functions and intervals | [references/datetime.md](references/datetime.md) |
| ARRAY, STRUCT, JSON/VARIANT types | [references/types.md](references/types.md) |
| BigQuery dialect quirks | [references/bigquery.md](references/bigquery.md) |

---

## Context

- You are querying a **semantic layer**, not a database directly.
- Only use model/view/column names defined in the MDL — never raw database table references.
- Wren Engine uses a generic SQL dialect similar to ANSI SQL (DataFusion/Postgres/DuckDB), but with differences.
- Check the `dataSource` field to identify the backend and apply dialect-specific rules if needed.

---

## Core SQL Rules

- Only `SELECT` statements. No `DELETE`, `UPDATE`, `INSERT`.
- Only use tables and columns from the MDL schema.
- Do not include comments in generated SQL.
- Prefer CTEs over subqueries.
- Identifiers are **case-sensitive**. Quote identifiers containing unicode, special characters (except `_`), or starting with a digit using double quotes.
  - Examples: `"客户"."姓名"`, `"table-name"."col"`, `"123column"`
- Identifier quotes: `"` (double quotes). String literal quotes: `'` (single quotes).
- For specific date queries, use a range:
  ```sql
  WHERE ts >= CAST('2024-11-01 00:00:00' AS TIMESTAMP WITH TIME ZONE)
    AND ts <  CAST('2024-11-02 00:00:00' AS TIMESTAMP WITH TIME ZONE)
  ```
- For ranking, use `DENSE_RANK()` + `WHERE`. Include the ranking column in `SELECT`.
- Avoid correlated subqueries — use JOINs instead.
- Use `SAFE_CAST` when casting might fail: `SAFE_CAST(col AS INT)`

---

## Filter Strategies

| Column type | Strategy |
|-------------|----------|
| Text | `LIKE '%value%'` for partial match |
| Numeric | `BETWEEN 30 AND 40` |
| Date/Timestamp | `>= '2024-01-01' AND < '2024-02-01'` |
| Exact value | `=` or `IN (...)` |
| Primary key / indexed | Prefer equality (`=`) |

---

## Supported Cast Types

`bool`, `boolean`, `int`, `integer`, `bigint`, `smallint`, `tinyint`, `float`, `double`, `real`, `decimal`, `numeric`, `varchar`, `char`, `string`, `text`, `date`, `time`, `timestamp`, `timestamp with time zone`, `bytea`

Example: `CAST(col AS INT)`, `TIMESTAMP '2024-11-09 00:00:00'`

---

## Aggregation

- All non-aggregated `SELECT` columns must appear in `GROUP BY` (window functions excepted).
- Aggregate conditions go in `HAVING`, not `WHERE`.
- Prefer ordinal `GROUP BY` for long column names:
  ```sql
  SELECT very_long_column_name AS alias, COUNT(*) FROM t GROUP BY 1
  ```

---

## Sorting and Limiting

- `ORDER BY` for sort; `LIMIT` to restrict rows.
- When `ORDER BY` appears in a subquery or CTE, always include `LIMIT`.

---

## Subquery Patterns

- Prefer CTEs (`WITH` clause) over nested subqueries.
- Subquery in `SELECT` must return a single value per row.
- Subquery in `WHERE`: use `IN`, `EXISTS`, or comparison operators.
- `IN SUBQUERY` in `JOIN` conditions is not supported — use `JOIN ... ON` instead.
- `RECURSIVE` CTEs are not supported.

---

## Calculated Fields

Columns marked as **Calculated Field** in the MDL have pre-defined computation logic. Use them directly instead of re-implementing the calculation.

Read the column comment (e.g., `column expression: avg(reviews.Score)`) to understand what the field represents.

```sql
-- Schema has: Rating DOUBLE (Calculated Field: avg(reviews.Score))
--             ReviewCount BIGINT (Calculated Field: count(reviews.Id))

-- Correct — use Calculated Fields directly:
SELECT AVG(Rating) FROM orders WHERE ReviewCount > 10

-- Incorrect — do not re-join and re-aggregate manually
```
