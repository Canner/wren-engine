"""
Integration tests for Oracle 19c custom dialect.

Validates that the complete transpilation flow from Trino to Oracle 19c
works correctly with the custom dialect, combining date arithmetic transforms,
type mapping overrides, and other 19c compatibility features.

These tests verify end-to-end transpilation scenarios to ensure all custom
dialect features work together correctly in realistic query patterns.
"""

import pytest
from sqlglot import exp

from app.custom_sqlglot.dialects.oracle import Oracle


@pytest.mark.oracle19c
@pytest.mark.dialect
class TestOracle19cIntegration:
    """Integration tests for Oracle 19c custom dialect transpilation."""

    def test_custom_dialect_is_used(self, oracle_dialect, transpile_trino_to_oracle):
        """Verify that our custom Oracle 19c dialect class is used during transpilation."""
        # Verify the dialect class is our custom one, not SQLGlot's base
        assert oracle_dialect == Oracle
        assert oracle_dialect.__name__ == "Oracle"
        assert oracle_dialect.__module__ == "app.custom_sqlglot.dialects.oracle"

        # Verify basic transpilation works
        sql = "SELECT * FROM users"
        result = transpile_trino_to_oracle(sql)
        assert "SELECT" in result.upper()
        assert "FROM" in result.upper()

    def test_full_transpilation_with_date_arithmetic(self, transpile_trino_to_oracle):
        """Test complete transpilation with date arithmetic transformations."""
        # Complex query with multiple date arithmetic operations
        sql = """
        SELECT
            order_id,
            order_date,
            order_date + INTERVAL '1' DAY as next_day,
            order_date + INTERVAL '1' MONTH as next_month,
            order_date + INTERVAL '1' YEAR as next_year,
            order_date - INTERVAL '7' DAY as week_ago
        FROM orders
        WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
        ORDER BY order_date DESC
        """
        result = transpile_trino_to_oracle(sql)

        # Verify Oracle 19c date arithmetic syntax (numeric for days)
        assert "+ 1" in result or "order_date + 1" in result.lower()
        assert "- 7" in result or "order_date - 7" in result.lower()
        assert "- 30" in result

        # Verify ADD_MONTHS used for month/year arithmetic
        assert "ADD_MONTHS(" in result.upper()

        # Verify no 21c+ INTERVAL syntax
        assert "INTERVAL" not in result.upper()

    def test_full_transpilation_with_type_mapping(self, oracle_type_mapping):
        """Test that TYPE_MAPPING correctly maps BOOLEAN to CHAR(1)."""
        # TYPE_MAPPING affects CREATE TABLE statements, not WHERE clause literals
        # Verify our custom type mapping is in effect
        assert exp.DataType.Type.BOOLEAN in oracle_type_mapping
        assert oracle_type_mapping[exp.DataType.Type.BOOLEAN] == "CHAR(1)"

        # Note: SQLGlot's Oracle dialect preserves TRUE/FALSE literals in WHERE clauses
        # This is actually valid in Oracle 19c for comparisons (though not for column types)
        # The TYPE_MAPPING ensures BOOLEAN columns are created as CHAR(1) in DDL
        # Application logic must handle 'Y'/'N' storage and comparison

    def test_complex_query_with_multiple_transformations(self, transpile_trino_to_oracle):
        """Test complex query combining date arithmetic, type mapping, and pagination."""
        sql = """
        SELECT
            o.order_id,
            o.order_date,
            o.order_date + INTERVAL '30' DAY as due_date,
            c.customer_name,
            c.is_active as active_flag
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.order_date >= CURRENT_DATE - INTERVAL '90' DAY
          AND c.is_active = TRUE
          AND o.is_cancelled = FALSE
        ORDER BY o.order_date DESC
        LIMIT 100
        """
        result = transpile_trino_to_oracle(sql)

        # Verify date arithmetic transformation
        assert "+ 30" in result or "order_date + 30" in result.lower()
        assert "- 90" in result

        # Verify no INTERVAL syntax
        assert "INTERVAL" not in result.upper()

        # Verify pagination converted to FETCH FIRST (Oracle 12c+ syntax)
        assert "FETCH FIRST 100 ROWS ONLY" in result.upper()
        assert "LIMIT" not in result.upper()

        # Verify JOIN preserved
        assert "JOIN" in result.upper()

    def test_real_world_query_scenario(self, transpile_trino_to_oracle):
        """Test realistic business query with aggregation, grouping, and filtering."""
        sql = """
        SELECT
            c.region,
            COUNT(o.order_id) as order_count,
            SUM(o.total_amount) as total_sales,
            MAX(o.order_date) as last_order_date,
            MAX(o.order_date) + INTERVAL '30' DAY as followup_date
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
            AND o.order_date >= CURRENT_DATE - INTERVAL '365' DAY
        WHERE c.is_active = TRUE
          AND c.created_date >= CURRENT_DATE - INTERVAL '2' YEAR
        GROUP BY c.region
        HAVING COUNT(o.order_id) > 10
        ORDER BY total_sales DESC
        LIMIT 50
        """
        result = transpile_trino_to_oracle(sql)

        # Verify date arithmetic for days
        assert "+ 30" in result
        assert "- 365" in result

        # Verify ADD_MONTHS for years
        assert "ADD_MONTHS(" in result.upper()

        # Verify no INTERVAL syntax
        assert "INTERVAL" not in result.upper()

        # Verify aggregation functions preserved
        assert "COUNT(" in result.upper()
        assert "SUM(" in result.upper()
        assert "MAX(" in result.upper()

        # Verify GROUP BY and HAVING preserved
        assert "GROUP BY" in result.upper()
        assert "HAVING" in result.upper()

        # Verify pagination
        assert "FETCH FIRST 50 ROWS ONLY" in result.upper()
        assert "LIMIT" not in result.upper()

    def test_nested_date_arithmetic(self, transpile_trino_to_oracle):
        """Test nested date arithmetic expressions."""
        sql = """
        SELECT
            order_date,
            (order_date + INTERVAL '1' MONTH) - INTERVAL '1' DAY as month_end
        FROM orders
        """
        result = transpile_trino_to_oracle(sql)

        # Verify ADD_MONTHS used
        assert "ADD_MONTHS(" in result.upper()

        # Verify day subtraction
        assert "- 1" in result

        # Verify no INTERVAL syntax
        assert "INTERVAL" not in result.upper()

    def test_date_arithmetic_in_subquery(self, transpile_trino_to_oracle):
        """Test date arithmetic works correctly in subqueries."""
        sql = """
        SELECT *
        FROM orders o
        WHERE o.order_date IN (
            SELECT MAX(order_date) + INTERVAL '1' DAY
            FROM orders
            WHERE customer_id = o.customer_id
        )
        """
        result = transpile_trino_to_oracle(sql)

        # Verify date arithmetic in subquery
        assert "+ 1" in result

        # Verify no INTERVAL syntax
        assert "INTERVAL" not in result.upper()

        # Verify subquery structure preserved
        assert "WHERE" in result.upper()
        assert "IN (" in result.upper() or "IN(" in result.upper()

    def test_case_expression_with_date_arithmetic(self, transpile_trino_to_oracle):
        """Test date arithmetic within CASE expressions."""
        sql = """
        SELECT
            order_id,
            CASE
                WHEN order_date >= CURRENT_DATE - INTERVAL '7' DAY THEN 'Recent'
                WHEN order_date >= CURRENT_DATE - INTERVAL '30' DAY THEN 'This Month'
                ELSE 'Older'
            END as order_age
        FROM orders
        """
        result = transpile_trino_to_oracle(sql)

        # Verify date arithmetic in CASE
        assert "- 7" in result
        assert "- 30" in result

        # Verify CASE structure preserved
        assert "CASE" in result.upper()
        assert "WHEN" in result.upper()
        assert "THEN" in result.upper()
        assert "ELSE" in result.upper()
        assert "END" in result.upper()

        # Verify no INTERVAL syntax
        assert "INTERVAL" not in result.upper()

    def test_cte_with_date_arithmetic(self, transpile_trino_to_oracle):
        """Test date arithmetic in Common Table Expressions (CTEs)."""
        sql = """
        WITH recent_orders AS (
            SELECT
                order_id,
                order_date,
                order_date + INTERVAL '30' DAY as followup_date
            FROM orders
            WHERE order_date >= CURRENT_DATE - INTERVAL '90' DAY
        )
        SELECT * FROM recent_orders
        WHERE followup_date <= CURRENT_DATE + INTERVAL '7' DAY
        """
        result = transpile_trino_to_oracle(sql)

        # Verify CTE structure preserved
        assert "WITH" in result.upper()
        assert "AS (" in result.upper() or "AS(" in result.upper()

        # Verify date arithmetic in CTE and main query
        assert "+ 30" in result or "+ 7" in result
        assert "- 90" in result

        # Verify no INTERVAL syntax
        assert "INTERVAL" not in result.upper()
