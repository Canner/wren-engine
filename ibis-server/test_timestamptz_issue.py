"""Test TIMESTAMPTZ and TO_DATE handling in Oracle dialects."""
import sqlglot
from app.custom_sqlglot.dialects.oracle import Oracle

# The problematic query from logs
sql_trino = """
SELECT
  CAST(TO_DATE("Estimated Date") AS TIMESTAMPTZ) "date"
, COUNT("SN Claim Primary Key") "claim_count"
FROM
  "RT SN Claim"
WHERE (CAST(TO_DATE("Estimated Date") AS TIMESTAMPTZ) <= CAST('2025-11-21 00:00:00' AS TIMESTAMPTZ))
GROUP BY CAST(TO_DATE("Estimated Date") AS TIMESTAMPTZ)
ORDER BY "date" ASC
"""

print("=== Original Trino SQL ===")
print(sql_trino)
print()

# Test 1: Default SQLGlot Oracle
print("=== Test 1: Default SQLGlot Oracle Dialect ===")
try:
    result_default = sqlglot.transpile(sql_trino.strip(), read='trino', write='oracle')
    print("SUCCESS:")
    print(result_default[0])
except Exception as e:
    print(f"ERROR: {e}")
print()

# Test 2: Our Custom Oracle 19c Dialect
print("=== Test 2: Custom Oracle 19c Dialect ===")
try:
    result_custom = sqlglot.transpile(sql_trino.strip(), read='trino', write=Oracle)
    print("SUCCESS:")
    print(result_custom[0])
except Exception as e:
    print(f"ERROR: {e}")
print()

# Test 3: What type does SQLGlot parse TIMESTAMPTZ as?
print("=== Test 3: Parse TIMESTAMPTZ Type ===")
parsed = sqlglot.parse_one("SELECT CAST(col AS TIMESTAMPTZ) FROM tbl", read='trino')
cast_node = parsed.find(sqlglot.exp.Cast)
if cast_node:
    data_type = cast_node.to
    print(f"Parsed type: {data_type}")
    print(f"Type enum: {data_type.this}")
print()

# Test 4: What does Oracle expect for timestamp with timezone?
print("=== Test 4: Oracle Timestamp Types ===")
test_types = [
    "SELECT CAST(col AS TIMESTAMP) FROM tbl",
    "SELECT CAST(col AS TIMESTAMP WITH TIME ZONE) FROM tbl",
    "SELECT CAST(col AS TIMESTAMPTZ) FROM tbl",
]
for sql in test_types:
    try:
        result = sqlglot.transpile(sql, read='oracle', write='oracle')
        print(f"Input:  {sql}")
        print(f"Output: {result[0]}")
    except Exception as e:
        print(f"Input:  {sql}")
        print(f"ERROR:  {e}")
    print()
