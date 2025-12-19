"""Quick test to validate Oracle 19c INTERVAL support claim."""
import sqlglot
from app.custom_sqlglot.dialects.oracle import Oracle

# Test 1: What does SQLGlot's default Oracle dialect generate?
sql_trino = "SELECT order_date + INTERVAL '1' DAY FROM orders"
result_default = sqlglot.transpile(sql_trino, read='trino', write='oracle')
print("=== Test 1: Default SQLGlot Oracle Dialect ===")
print(f"Input (Trino):  {sql_trino}")
print(f"Output (Oracle): {result_default[0]}\n")

# Test 2: What does our custom dialect generate?
result_custom = sqlglot.transpile(sql_trino, read='trino', write=Oracle)
print("=== Test 2: Custom Oracle 19c Dialect ===")
print(f"Input (Trino):  {sql_trino}")
print(f"Output (Oracle 19c): {result_custom[0]}\n")

# Test 3: What if we parse Oracle syntax directly?
sql_oracle = "SELECT systimestamp + INTERVAL '1' DAY FROM dual"
parsed = sqlglot.parse_one(sql_oracle, read='oracle')
print("=== Test 3: Parse Oracle INTERVAL Syntax ===")
print(f"Input:  {sql_oracle}")
print(f"Parsed: {parsed}")
print(f"AST Type: {type(parsed.find(sqlglot.exp.Add))}\n")

# Test 4: Can SQLGlot read and write Oracle INTERVAL syntax?
result_oracle_to_oracle = sqlglot.transpile(sql_oracle, read='oracle', write='oracle')
print("=== Test 4: Oracle → Oracle (No Transpilation) ===")
print(f"Input:  {sql_oracle}")
print(f"Output: {result_oracle_to_oracle[0]}\n")

# Test 5: What about month intervals?
sql_month = "SELECT hire_date + INTERVAL '1' MONTH FROM employees"
result_month = sqlglot.transpile(sql_month, read='trino', write='oracle')
print("=== Test 5: MONTH Interval ===")
print(f"Input (Trino):  {sql_month}")
print(f"Output (Oracle): {result_month[0]}\n")
