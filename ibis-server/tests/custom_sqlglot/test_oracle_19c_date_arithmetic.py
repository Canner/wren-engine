"""
Unit tests for Oracle 19c custom dialect date arithmetic transformations.

Tests validate that the custom Oracle 19c dialect correctly converts INTERVAL-based
date arithmetic expressions into Oracle 19c-compatible syntax:
- DAY units: Numeric addition/subtraction (date ± n)
- MONTH units: ADD_MONTHS(date, ±n)
- YEAR units: ADD_MONTHS(date, ±(n * 12))

Implements: SCAIS-23 P3.001 (TEST-001, COMP-003)
"""

import pytest
import sqlglot
from app.custom_sqlglot.dialects.oracle import Oracle


@pytest.mark.oracle19c
@pytest.mark.date_arithmetic
class TestOracle19cDateArithmetic:
    """Unit tests for Oracle 19c date arithmetic transformations."""

    def test_date_add_day_simple(self, transpile_trino_to_oracle):
        """Test date + INTERVAL '1' DAY converts to numeric addition."""
        sql = "SELECT hire_date + INTERVAL '1' DAY FROM employees"
        result = transpile_trino_to_oracle(sql)

        assert "+ 1" in result
        assert "INTERVAL" not in result.upper()

    def test_date_add_day_multiple(self, transpile_trino_to_oracle):
        """Test date addition with multiple days (7 and 30 days)."""
        # Test 7 days
        sql = "SELECT order_date + INTERVAL '7' DAY FROM orders"
        result = transpile_trino_to_oracle(sql)

        assert "+ 7" in result
        assert "INTERVAL" not in result.upper()

        # Test 30 days
        sql = "SELECT created_at + INTERVAL '30' DAY FROM users"
        result = transpile_trino_to_oracle(sql)

        assert "+ 30" in result
        assert "INTERVAL" not in result.upper()

    def test_date_sub_day_simple(self, transpile_trino_to_oracle):
        """Test date - INTERVAL '1' DAY converts to numeric subtraction."""
        sql = "SELECT hire_date - INTERVAL '1' DAY FROM employees"
        result = transpile_trino_to_oracle(sql)

        assert "- 1" in result
        assert "INTERVAL" not in result.upper()

    def test_date_sub_day_multiple(self, transpile_trino_to_oracle):
        """Test date subtraction with multiple days."""
        sql = "SELECT order_date - INTERVAL '14' DAY FROM orders"
        result = transpile_trino_to_oracle(sql)

        assert "- 14" in result
        assert "INTERVAL" not in result.upper()

    def test_date_add_month(self, transpile_trino_to_oracle):
        """Test date + INTERVAL 'n' MONTH converts to ADD_MONTHS."""
        sql = "SELECT hire_date + INTERVAL '3' MONTH FROM employees"
        result = transpile_trino_to_oracle(sql)

        assert "ADD_MONTHS(" in result.upper()
        assert ", 3)" in result
        assert "INTERVAL" not in result.upper()

    def test_date_sub_month(self, transpile_trino_to_oracle):
        """Test date - INTERVAL 'n' MONTH converts to ADD_MONTHS with negative."""
        sql = "SELECT hire_date - INTERVAL '2' MONTH FROM employees"
        result = transpile_trino_to_oracle(sql)

        assert "ADD_MONTHS(" in result.upper()
        assert ", -2)" in result
        assert "INTERVAL" not in result.upper()

    def test_date_add_year(self, transpile_trino_to_oracle):
        """Test date + INTERVAL 'n' YEAR converts to ADD_MONTHS(date, n * 12)."""
        sql = "SELECT hire_date + INTERVAL '2' YEAR FROM employees"
        result = transpile_trino_to_oracle(sql)

        assert "ADD_MONTHS(" in result.upper()
        assert "* 12)" in result
        assert ", 2 * 12)" in result or ", 2*12)" in result.replace(" ", "")
        assert "INTERVAL" not in result.upper()

    def test_date_sub_year(self, transpile_trino_to_oracle):
        """Test date - INTERVAL 'n' YEAR converts to ADD_MONTHS with negative."""
        sql = "SELECT hire_date - INTERVAL '1' YEAR FROM employees"
        result = transpile_trino_to_oracle(sql)

        assert "ADD_MONTHS(" in result.upper()
        # Should have parentheses: -(n * 12)
        assert "-(1 * 12)" in result or "-( 1 * 12 )" in result or "-(1*12)" in result.replace(" ", "")
        assert "INTERVAL" not in result.upper()

    def test_date_arithmetic_in_where_clause(self, transpile_trino_to_oracle):
        """Test date arithmetic in WHERE clause filters."""
        sql = """
        SELECT * FROM orders
        WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY
        """
        result = transpile_trino_to_oracle(sql)

        assert "CURRENT_DATE - 7" in result or "SYSDATE - 7" in result.upper()
        assert "INTERVAL" not in result.upper()

    def test_mixed_date_operations(self, transpile_trino_to_oracle):
        """Test query with multiple date arithmetic operations."""
        sql = """
        SELECT
            hire_date + INTERVAL '1' DAY as next_day,
            hire_date + INTERVAL '3' MONTH as three_months_later,
            hire_date - INTERVAL '1' YEAR as last_year
        FROM employees
        """
        result = transpile_trino_to_oracle(sql)

        # Should have all three transformations
        assert "+ 1" in result  # DAY addition
        assert "ADD_MONTHS(" in result.upper()  # MONTH/YEAR use ADD_MONTHS
        assert "INTERVAL" not in result.upper()  # No INTERVAL keyword

    def test_date_add_with_column_names(self, transpile_trino_to_oracle):
        """Test date arithmetic preserves column names correctly."""
        sql = "SELECT start_date + INTERVAL '90' DAY as end_date FROM projects"
        result = transpile_trino_to_oracle(sql)

        assert "start_date" in result.lower() or "START_DATE" in result
        assert "+ 90" in result
        assert "end_date" in result.lower() or "END_DATE" in result

    def test_date_arithmetic_in_select_expression(self, transpile_trino_to_oracle):
        """Test date arithmetic in complex SELECT expressions."""
        sql = """
        SELECT
            order_id,
            order_date,
            order_date + INTERVAL '30' DAY as due_date,
            order_date - INTERVAL '7' DAY as reminder_date
        FROM orders
        WHERE status = 'PENDING'
        """
        result = transpile_trino_to_oracle(sql)

        assert "+ 30" in result  # DAY addition
        assert "- 7" in result  # DAY subtraction
        assert "INTERVAL" not in result.upper()
        assert "PENDING" in result

    def test_date_month_arithmetic_edge_case(self, transpile_trino_to_oracle):
        """Test month arithmetic handles edge cases (month-end dates)."""
        # ADD_MONTHS handles Jan 31 + 1 month = Feb 28/29 correctly
        sql = "SELECT invoice_date + INTERVAL '1' MONTH FROM invoices"
        result = transpile_trino_to_oracle(sql)

        assert "ADD_MONTHS(" in result.upper()
        assert ", 1)" in result
        # Verify ADD_MONTHS is used (handles edge cases correctly)

    def test_date_year_subtraction_with_multiple_years(self, transpile_trino_to_oracle):
        """Test year subtraction with multiple years maintains correct syntax."""
        sql = "SELECT created_at - INTERVAL '5' YEAR FROM accounts"
        result = transpile_trino_to_oracle(sql)

        assert "ADD_MONTHS(" in result.upper()
        assert "-(5 * 12)" in result or "-( 5 * 12 )" in result or "-(5*12)" in result.replace(" ", "")
        assert "INTERVAL" not in result.upper()
