"""Test SQLGlot's DEFAULT Oracle dialect behavior (not our custom one)."""
import sqlglot

# Temporarily test WITHOUT our custom dialect
# Use sqlglot.dialects.oracle.Oracle directly

# Test: What does ORIGINAL SQLGlot generate for Oracle?
sql_oracle_interval = "SELECT systimestamp + INTERVAL '1' DAY FROM dual"
result = sqlglot.transpile(sql_oracle_interval, read='oracle', write='oracle')

print("=== SQLGlot DEFAULT Oracle Dialect (No Custom Override) ===")
print(f"Input:  {sql_oracle_interval}")
print(f"Output: {result[0]}")
print()

# Test: Can it preserve INTERVAL syntax?
parsed = sqlglot.parse_one(sql_oracle_interval, read='oracle')
print("=== Parse Tree ===")
print(parsed.sql(dialect='oracle'))
print()

# Test: What does Trino → Oracle look like?
sql_trino = "SELECT order_date + INTERVAL '1' DAY FROM orders"
result_trino = sqlglot.transpile(sql_trino, read='trino', write='oracle')
print("=== Trino → Default Oracle ===")
print(f"Input (Trino):  {sql_trino}")
print(f"Output (Oracle): {result_trino[0]}")
