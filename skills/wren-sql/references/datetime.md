# Date and Time Functions

## Current Values

```sql
CURRENT_DATE       -- current date
CURRENT_TIMESTAMP  -- current timestamp
```

## Truncation

```sql
DATE_TRUNC('<part>', <timestamp>)
```

Parts: `'year'`, `'quarter'`, `'month'`, `'week'`, `'day'`, `'hour'`, `'minute'`, `'second'`

## Extraction

```sql
EXTRACT(<part> FROM <timestamp>)
```

Parts: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`

## Date Difference

```sql
DATE_DIFF('<part>', <start_date>, <end_date>)
```

Parts: `'year'`, `'quarter'`, `'month'`, `'week'`, `'day'`, `'hour'`, `'minute'`, `'second'`

## Interval Arithmetic

```sql
<date_column>      + INTERVAL '7' days
<timestamp_column> - INTERVAL '3' hours
```

## Timezone

- Use `TIMESTAMP WITH TIME ZONE` when timezone matters.
- `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE` cannot be compared directly — cast one to the other first:
  ```sql
  CAST(<timestamp_col> AS TIMESTAMP WITH TIME ZONE)
  CAST(<timestamp_tz_col> AS TIMESTAMP)
  ```

## Epoch / Unix Time Conversion

If a column stores dates/timestamps as integers, convert using:

```sql
to_timestamp(<int_col>)         -- seconds
to_timestamp_millis(<int_col>)  -- milliseconds
to_timestamp_micros(<int_col>)  -- microseconds
to_timestamp_nanos(<int_col>)   -- nanoseconds
to_timestamp_seconds(<int_col>) -- seconds (explicit)
```
