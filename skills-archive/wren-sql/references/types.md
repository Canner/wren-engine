# Complex Data Types

## ARRAY

### Literals

```sql
ARRAY[<value1>, <value2>, ..., <valueN>]
['value1', 'value2', ..., 'valueN']
CAST(ARRAY[<value1>, ..., <valueN>] AS ARRAY<data_type>)
CAST(['value1', ..., 'valueN'] AS <data_type>[])
```

### Type Definition

```sql
ARRAY<data_type>
<data_type>[]
```

### UNNEST — same table

```sql
SELECT ...
FROM <table_name>, UNNEST(<array_column>) AS <alias>(<alias_col>)
```

### UNNEST — join with another table

```sql
SELECT ...
FROM <table1>
JOIN <table2>, UNNEST(<table2>.<array_col>) AS <alias>(<alias_col>)
  ON <table1>.<col> = <alias>.<alias_col>
```

---

## STRUCT

### Type Definition

```sql
STRUCT<field1 data_type1, field2 data_type2, ..., fieldN data_typeN>
```

### Accessing Fields

Use dot notation:

```sql
<struct_column>.<field_name>

-- Example:
SELECT address.city, address.postcode FROM users
```

---

## Semi-Structured Types (JSON / VARIANT / OBJECT)

### Type Definitions

`JSON`, `VARIANT`, `OBJECT` — no fixed schema. Don't assume specific fields unless confirmed via column metadata, comments, or sample data. Account for missing fields or varying structure across rows.

### Accessing Fields

Do **not** use dot notation. Use `GET_PATH`:

```sql
GET_PATH(<semi_structured_column>, '<json_path>')
```

### Type Conversion After GET_PATH

Do **not** use `CAST` directly on semi-structured values. Use type-specific functions:

```sql
AS_BOOLEAN(GET_PATH(...))   -- boolean
AS_DOUBLE(GET_PATH(...))    -- double/float
AS_INTEGER(GET_PATH(...))   -- bigint
AS_VARCHAR(GET_PATH(...))   -- varchar

-- Example:
AS_VARCHAR(GET_PATH(address_col, '$.city'))
```

### Creating Semi-Structured Literals

```sql
parse_json('{"field1": "value1", "field2": 123}')
```

### UNNEST Arrays Inside Semi-Structured Data

```sql
SELECT item
FROM <table_name>,
  UNNEST(AS_ARRAY(GET_PATH(<table>.<semi_col>, '<json_path>'))) AS unnest_alias(item)
```

### JSON Type Rules (from column metadata)

If column metadata specifies `json_type`:

| `json_type` value | How to access |
|-------------------|---------------|
| `JSON` | `GET_PATH(col, '$.field')` |
| `JSON_ARRAY` | `AS_ARRAY(GET_PATH(col, '$.field'))` |
| *(empty)* | Direct column reference, no `GET_PATH` |

Follow the `path` property in metadata to build the correct path expression.

### Example

Schema metadata:
```sql
-- {"json_type":"JSON","json_fields":{"address.json.city":{"path":"$.city","type":"varchar"}}}
address JSON
```

Query:
```sql
SELECT AS_VARCHAR(GET_PATH(u.address, '$.city')) FROM users AS u
```

> Complex queries on semi-structured columns are slower than on structured columns. Optimize accordingly.
