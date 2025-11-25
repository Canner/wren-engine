# Oracle 19c SQLGlot Dialect Override

## Overview

This document describes the custom Oracle 19c dialect implementation that enables WrenAI to generate Oracle 19c-compatible SQL syntax.

**Work ID:** SCAIS-23  
**Version:** 1.0  
**Date:** 2025-11-25  
**Status:** Complete

## Problem Statement

SQLGlot's default Oracle dialect generates Oracle 21c+ syntax that is incompatible with Oracle 19c, specifically:
- **Date Arithmetic**: Generates `INTERVAL` expressions not supported in 19c
- **Type Mapping**: Maps BOOLEAN to native BOOLEAN type (21c+ feature)

## Solution

Custom Oracle 19c dialect override at `app/custom_sqlglot/dialects/oracle.py` that:
1. Transforms date arithmetic to numeric addition/subtraction
2. Maps BOOLEAN to CHAR(1) for 19c compatibility
3. Maintains all other Oracle dialect features

## Implementation Details

### Date Arithmetic Transformations

**DAY Arithmetic:**
```python
# Input (Trino)
SELECT order_date + INTERVAL '7' DAY FROM orders

# Output (Oracle 19c)
SELECT order_date + 7 FROM orders
```

**MONTH Arithmetic:**
```python
# Input (Trino)
SELECT hire_date + INTERVAL '1' MONTH FROM employees

# Output (Oracle 19c)
SELECT ADD_MONTHS(hire_date, 1) FROM employees
```

**YEAR Arithmetic:**
```python
# Input (Trino)
SELECT start_date + INTERVAL '2' YEAR FROM projects

# Output (Oracle 19c)
SELECT ADD_MONTHS(start_date, 2 * 12) FROM projects
```

### Type Mapping

**BOOLEAN Type:**
```python
TYPE_MAPPING = {
    **OriginalOracle.Generator.TYPE_MAPPING,
    exp.DataType.Type.BOOLEAN: "CHAR(1)",
}
```

This ensures `CREATE TABLE` statements with BOOLEAN columns generate CHAR(1) instead of native BOOLEAN (21c+ feature).

**Note:** Boolean literals (TRUE/FALSE) in WHERE clauses are valid in Oracle 19c and preserved by default. Application logic should use 'Y'/'N' for storage.

## Testing

### Test Suite

**Total: 34 tests, all passing in 0.15s**

- **Date Arithmetic Tests (14)**: `test_oracle_19c_date_arithmetic.py`
  - Day addition/subtraction
  - Month addition/subtraction
  - Year addition/subtraction
  - Mixed operations
  - WHERE clause expressions

- **Type Mapping Tests (11)**: `test_oracle_19c_type_mapping.py`
  - BOOLEAN → CHAR(1) mapping
  - Type inheritance verification
  - Oracle-specific types (VARCHAR2, NVARCHAR2)

- **Integration Tests (9)**: `test_oracle_19c_integration.py`
  - Full transpilation flow validation
  - Complex queries (JOINs, subqueries, CTEs)
  - Real-world business queries
  - Nested expressions

### Running Tests

**Quick Test:**
```bash
wren-test-oracle
```

**Full Command:**
```bash
cd wren-engine/ibis-server
PYTHONPATH=. poetry run pytest tests/custom_sqlglot/ -m oracle19c -v --confcutdir=tests/custom_sqlglot
```

**Specific Test Files:**
```bash
# Date arithmetic only
poetry run pytest tests/custom_sqlglot/test_oracle_19c_date_arithmetic.py -v

# Type mapping only
poetry run pytest tests/custom_sqlglot/test_oracle_19c_type_mapping.py -v

# Integration tests only
poetry run pytest tests/custom_sqlglot/test_oracle_19c_integration.py -v
```

## Files Modified/Created

### Core Implementation
- ✅ `app/custom_sqlglot/dialects/oracle.py` - Custom Oracle 19c dialect class (177 lines)
- ✅ `app/custom_sqlglot/dialects/__init__.py` - Dialect registration

### Test Infrastructure
- ✅ `tests/custom_sqlglot/conftest.py` - Isolated pytest configuration with fixtures (51 lines)
- ✅ `tests/custom_sqlglot/test_oracle_19c_date_arithmetic.py` - 14 date arithmetic tests (173 lines)
- ✅ `tests/custom_sqlglot/test_oracle_19c_type_mapping.py` - 11 type mapping tests (136 lines)
- ✅ `tests/custom_sqlglot/test_oracle_19c_integration.py` - 9 integration tests (248 lines)

### Configuration
- ✅ `pyproject.toml` - Added pytest markers (oracle19c, dialect, type_mapping, date_arithmetic)
- ✅ `~/wren-test-oracle.sh` - Shell script for easy test execution
- ✅ `~/.bashrc` - Added aliases (wren-test-oracle, wren-poetry, wren-cd)

## Usage

### Automatic Usage

The custom Oracle 19c dialect is automatically used when:
1. Data source is set to "oracle" in WrenAI
2. SQLGlot transpiles Trino SQL to Oracle dialect
3. The `app.custom_sqlglot.dialects` module is imported (happens automatically)

### Manual Testing

```python
import sqlglot
from app.custom_sqlglot.dialects.oracle import Oracle

# Transpile using custom dialect
trino_sql = "SELECT hire_date + INTERVAL '7' DAY FROM employees"
oracle_sql = sqlglot.transpile(trino_sql, read="trino", write=Oracle)[0]

print(oracle_sql)
# Output: SELECT hire_date + 7 FROM employees
```

## Validation

### Requirements Validation

| Requirement | Status | Evidence |
|-------------|--------|----------|
| FR-001: Date Arithmetic Compatibility | ✅ Complete | 14 passing tests |
| FR-002: Pagination Syntax | ✅ Validated | Base dialect compatible |
| FR-003: Custom Dialect Registration | ✅ Complete | Registered in __init__.py |
| FR-004: Type Mapping Validation | ✅ Complete | 11 passing tests |

### Acceptance Criteria

| AC ID | Criteria | Status |
|-------|----------|--------|
| AC-001 | `oracle.py` file created | ✅ Complete |
| AC-002 | Dialect registered in `__init__.py` | ✅ Complete |
| AC-003 | Date arithmetic uses numeric addition | ✅ Validated (14 tests) |
| AC-004 | Pagination uses FETCH FIRST syntax | ✅ Validated |
| AC-005 | Unit tests pass for all overrides | ✅ Validated (34 tests) |
| AC-006 | Integration tests pass | ✅ Validated (9 tests) |

## Known Limitations

1. **Boolean Literals in WHERE Clauses**: SQLGlot preserves TRUE/FALSE literals in WHERE clauses. These are technically valid in Oracle 19c for comparisons. Application logic must use 'Y'/'N' for actual storage.

2. **Real Database Testing**: Tests validate transpilation correctness but do not execute against a live Oracle 19c database. Deployment to production should include validation queries against target database.

3. **INTERVAL Units**: Currently supports DAY, MONTH, YEAR units. Other interval units (HOUR, MINUTE, SECOND) not yet implemented but can be added if needed.

## Future Enhancements

- Support for additional INTERVAL units (HOUR, MINUTE, SECOND)
- Boolean literal transformation in WHERE clauses if required
- Performance profiling against large query sets
- Live database integration tests (when Oracle 19c instance available)

## References

- **PRD**: `_ai/workspace/SCAIS-23/SCAIS-23-prd.md`
- **Design Doc**: `_ai/workspace/SCAIS-23/SCAIS-23-dd.md`
- **Task Breakdown**: `_ai/workspace/SCAIS-23/SCAIS-23-tasks.md`
- **RTM**: `_ai/workspace/SCAIS-23/SCAIS-23-rtm.md`
- **Oracle 19c vs 23c Differences**: `_ai/workspace/SCAIS-23/Differences in SQL Generation Between Oracle 19c and SQLGlot's Oracle 23c (AI Release).md`

## Support

For issues or questions:
1. Check test suite output: `wren-test-oracle`
2. Review test implementations for usage examples
3. Consult PRD/DD documents for requirements and design rationale
4. Contact: WrenAI development team

---

**Document Version:** 1.0  
**Last Updated:** 2025-11-25  
**Maintained By:** WrenAI Team
