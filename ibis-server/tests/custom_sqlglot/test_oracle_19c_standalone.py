"""
Standalone unit tests for Oracle 19c date arithmetic - no app dependencies.

This test file can run independently without requiring the full WrenAI app setup.
It only tests the SQLGlot dialect transformations in isolation.
"""

import sys
from pathlib import Path

# Add app to path without importing the full application
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import sqlglot
from app.custom_sqlglot.dialects.oracle import Oracle


def test_date_add_day_simple():
    """Test date + INTERVAL '1' DAY converts to numeric addition."""
    sql = 'SELECT hire_date + INTERVAL \'1\' DAY FROM employees'
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    assert "+ 1" in result
    assert "INTERVAL" not in result.upper()
    print("✓ test_date_add_day_simple passed")


def test_date_add_day_multiple():
    """Test date addition with multiple days."""
    sql = 'SELECT order_date + INTERVAL \'7\' DAY FROM orders'
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    assert "+ 7" in result
    assert "INTERVAL" not in result.upper()
    print("✓ test_date_add_day_multiple passed")


def test_date_sub_day_simple():
    """Test date - INTERVAL '1' DAY converts to numeric subtraction."""
    sql = 'SELECT hire_date - INTERVAL \'1\' DAY FROM employees'
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    assert "- 1" in result
    assert "INTERVAL" not in result.upper()
    print("✓ test_date_sub_day_simple passed")


def test_date_add_month():
    """Test date + INTERVAL 'n' MONTH converts to ADD_MONTHS."""
    sql = 'SELECT hire_date + INTERVAL \'3\' MONTH FROM employees'
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    assert "ADD_MONTHS(" in result.upper()
    assert ", 3)" in result
    assert "INTERVAL" not in result.upper()
    print("✓ test_date_add_month passed")


def test_date_sub_month():
    """Test date - INTERVAL 'n' MONTH converts to ADD_MONTHS with negative."""
    sql = 'SELECT hire_date - INTERVAL \'2\' MONTH FROM employees'
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    assert "ADD_MONTHS(" in result.upper()
    assert ", -2)" in result
    assert "INTERVAL" not in result.upper()
    print("✓ test_date_sub_month passed")


def test_date_add_year():
    """Test date + INTERVAL 'n' YEAR converts to ADD_MONTHS(date, n * 12)."""
    sql = 'SELECT hire_date + INTERVAL \'2\' YEAR FROM employees'
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    assert "ADD_MONTHS(" in result.upper()
    assert "* 12)" in result
    assert "INTERVAL" not in result.upper()
    print("✓ test_date_add_year passed")


def test_date_sub_year():
    """Test date - INTERVAL 'n' YEAR converts to ADD_MONTHS with negative."""
    sql = 'SELECT hire_date - INTERVAL \'1\' YEAR FROM employees'
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    assert "ADD_MONTHS(" in result.upper()
    assert "* 12" in result
    assert "INTERVAL" not in result.upper()
    print("✓ test_date_sub_year passed")


def test_date_arithmetic_in_where_clause():
    """Test date arithmetic in WHERE clause filters."""
    sql = '''
    SELECT * FROM orders 
    WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY
    '''
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    # Should convert to numeric subtraction
    assert ("- 7" in result or "SYSDATE - 7" in result.upper())
    assert "INTERVAL" not in result.upper()
    print("✓ test_date_arithmetic_in_where_clause passed")


def test_mixed_date_operations():
    """Test query with multiple date arithmetic operations."""
    sql = '''
    SELECT 
        hire_date + INTERVAL '1' DAY as next_day,
        hire_date + INTERVAL '3' MONTH as three_months_later,
        hire_date - INTERVAL '1' YEAR as last_year
    FROM employees
    '''
    result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]
    
    # Should have all three transformations
    assert "+ 1" in result  # DAY addition
    assert "ADD_MONTHS(" in result.upper()  # MONTH/YEAR use ADD_MONTHS
    assert "INTERVAL" not in result.upper()  # No INTERVAL keyword
    print("✓ test_mixed_date_operations passed")


if __name__ == "__main__":
    print("Running Oracle 19c Date Arithmetic Tests\n")
    print("=" * 60)
    
    tests = [
        test_date_add_day_simple,
        test_date_add_day_multiple,
        test_date_sub_day_simple,
        test_date_add_month,
        test_date_sub_month,
        test_date_add_year,
        test_date_sub_year,
        test_date_arithmetic_in_where_clause,
        test_mixed_date_operations,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ {test.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {test.__name__} ERROR: {e}")
            failed += 1
    
    print("=" * 60)
    print(f"\nResults: {passed} passed, {failed} failed out of {len(tests)} total")
    
    if failed == 0:
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print(f"\n❌ {failed} test(s) failed")
        sys.exit(1)
