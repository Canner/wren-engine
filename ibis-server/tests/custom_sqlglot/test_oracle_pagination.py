"""
Test Oracle pagination syntax compatibility.

Validates that SQLGlot's default Oracle dialect generates FETCH FIRST syntax
(Oracle 12c+) which is compatible with Oracle 19c. No custom pagination
implementation is needed.
"""

import pytest
import sqlglot


class TestOraclePaginationSyntax:
    """Test that pagination uses Oracle 12c+ FETCH FIRST syntax."""

    def test_limit_converts_to_fetch_first(self):
        """Verify LIMIT converts to FETCH FIRST (19c-compatible)."""
        sql = "SELECT * FROM users LIMIT 10"
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        assert "FETCH FIRST 10 ROWS ONLY" in result.upper()
        assert "LIMIT" not in result.upper()

    def test_limit_with_offset(self):
        """Verify LIMIT with OFFSET uses 19c-compatible syntax."""
        sql = "SELECT * FROM users LIMIT 10 OFFSET 20"
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        assert "OFFSET 20 ROWS" in result.upper()
        assert "FETCH FIRST 10 ROWS ONLY" in result.upper()

    def test_limit_only(self):
        """Verify simple LIMIT clause converts correctly."""
        sql = "SELECT id, name FROM products ORDER BY price DESC LIMIT 5"
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        assert "FETCH FIRST 5 ROWS ONLY" in result.upper()
        assert "LIMIT" not in result.upper()
        assert "ORDER BY" in result.upper()

    def test_offset_without_limit(self):
        """Verify OFFSET alone is handled correctly."""
        sql = "SELECT * FROM orders OFFSET 50"
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        # OFFSET requires FETCH FIRST in Oracle
        assert "OFFSET" in result.upper()

    def test_pagination_with_where_clause(self):
        """Verify pagination works with WHERE conditions."""
        sql = "SELECT * FROM customers WHERE active = TRUE LIMIT 20 OFFSET 10"
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        assert "OFFSET 10 ROWS" in result.upper()
        assert "FETCH FIRST 20 ROWS ONLY" in result.upper()
        assert "LIMIT" not in result.upper()

    def test_pagination_preserves_order_by(self):
        """Verify ORDER BY is preserved with pagination."""
        sql = "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10"
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        assert "ORDER BY" in result.upper()
        assert "SALARY" in result.upper()
        assert "FETCH FIRST 10 ROWS ONLY" in result.upper()

    def test_large_limit_value(self):
        """Verify large LIMIT values are handled correctly."""
        sql = "SELECT * FROM transactions LIMIT 1000"
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        assert "FETCH FIRST 1000 ROWS ONLY" in result.upper()

    def test_pagination_with_joins(self):
        """Verify pagination works with JOIN clauses."""
        sql = """
        SELECT o.id, c.name 
        FROM orders o 
        JOIN customers c ON o.customer_id = c.id 
        LIMIT 50 OFFSET 100
        """
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        assert "OFFSET 100 ROWS" in result.upper()
        assert "FETCH FIRST 50 ROWS ONLY" in result.upper()
        assert "JOIN" in result.upper()

    def test_no_pagination_clause(self):
        """Verify queries without pagination work normally."""
        sql = "SELECT * FROM users"
        result = sqlglot.transpile(sql, read="trino", write="oracle")[0]

        # Should not have pagination syntax
        assert "FETCH FIRST" not in result.upper()
        assert "OFFSET" not in result.upper()
        assert "LIMIT" not in result.upper()


class TestOracleCustomDialectPagination:
    """Test pagination with custom Oracle 19c dialect."""

    def test_custom_dialect_preserves_fetch_first(self):
        """Verify custom Oracle dialect doesn't break pagination."""
        from app.custom_sqlglot.dialects.oracle import Oracle

        sql = "SELECT * FROM users LIMIT 10"
        result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]

        assert "FETCH FIRST 10 ROWS ONLY" in result.upper()
        assert "LIMIT" not in result.upper()

    def test_custom_dialect_pagination_with_date_arithmetic(self):
        """Verify pagination works with custom date arithmetic transforms."""
        from app.custom_sqlglot.dialects.oracle import Oracle

        sql = """
        SELECT * FROM orders 
        WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY 
        ORDER BY order_date DESC 
        LIMIT 20
        """
        result = sqlglot.transpile(sql, read="trino", write=Oracle)[0]

        # Should have both date arithmetic fix AND pagination
        assert "FETCH FIRST 20 ROWS ONLY" in result.upper()
        # Date arithmetic should use numeric subtraction (from P2.002-P2.006)
        assert "- 7" in result or "ADD_MONTHS" in result.upper()
        assert "LIMIT" not in result.upper()
