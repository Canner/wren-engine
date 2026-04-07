# BigQuery Dialect Rules

Apply these rules when generating or correcting Wren SQL with BigQuery as the backend database.

## TIMESTAMP + INTERVAL with MONTH / YEAR

`TIMESTAMP WITH TIME ZONE` cannot use `+`/`-` with INTERVAL containing non-zero MONTH or YEAR parts.

```sql
-- Valid:
timestamp_col + INTERVAL '7' days

-- Invalid:
timestamp_col + INTERVAL '1' month
timestamp_col - INTERVAL '2' year

-- Fix — cast to TIMESTAMP first:
CAST(timestamp_col AS TIMESTAMP) + INTERVAL '1' month
```

## STRING vs NUMERIC Comparison

`STRING` cannot be compared with `INTEGER` or `FLOAT` directly. Use `SAFE_CAST` or `CAST`:

```sql
SAFE_CAST(string_col AS INT) > 100
CAST(string_col AS FLOAT) <> 75.5
```

## Parsing String to Timestamp

```sql
PARSE_DATETIME('%Y-%m-%d', string_col)              -- timestamp (no timezone)
PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', string_col)    -- timestamp with timezone
```

## GROUP BY Alias

BigQuery does not allow `GROUP BY` to reference aliases defined in `SELECT`.

```sql
-- Invalid:
SELECT col1 AS alias1, COUNT(*) FROM t GROUP BY alias1

-- Valid:
SELECT col1 AS alias1, COUNT(*) FROM t GROUP BY col1

-- Preferred for long names:
SELECT very_long_column AS alias, COUNT(*) FROM t GROUP BY 1
```

## Column Name Same as Table Name

If a column name matches the table name (case-insensitive), rename with an alias to avoid ambiguity.

```sql
-- Ambiguous:
SELECT "User".user FROM "User"

-- Clear:
SELECT "User".user AS user_column FROM "User"
```

## Date/Time Diff Functions — Argument Order

`DATE_DIFF`, `TIMESTAMP_DIFF`, `DATETIME_DIFF`, `TIME_DIFF` take arguments as `(part, start, end)`:

```sql
DATE_DIFF('day', start_date, end_date)
TIMESTAMP_DIFF('hour', start_ts, end_ts)
```

## TIMESTAMP_DIFF with YEAR / MONTH

`TIMESTAMP_DIFF` does not support `year` or `month` date parts for TIMESTAMP arguments. Cast to DATE first:

```sql
-- Invalid:
TIMESTAMP_DIFF('year', ts1, ts2)

-- Fix:
TIMESTAMP_DIFF('year', CAST(ts1 AS DATE), CAST(ts2 AS DATE))
```

## DATETIME vs TIMESTAMP

- `DATETIME` — local time, no timezone
- `TIMESTAMP` — UTC-based, no timezone in display
- `TIMESTAMP WITH TIME ZONE` — use when timezone awareness is required
