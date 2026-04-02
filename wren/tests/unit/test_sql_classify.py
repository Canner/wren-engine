"""Unit tests for sql_classify.is_exploratory()."""

import pytest

from wren.sql_classify import is_exploratory

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "sql, expected",
    [
        # Exploratory: bare SELECT + LIMIT, no WHERE/GROUP/HAVING/agg
        ("SELECT * FROM orders LIMIT 5", True),
        ("SELECT DISTINCT status FROM orders LIMIT 10", True),
        # No LIMIT — full scan is intentional, not exploratory
        ("SELECT * FROM orders", False),
        # Aggregate present
        ("SELECT status, COUNT(*) FROM orders GROUP BY 1", False),
        # WHERE present
        ("SELECT * FROM orders WHERE total > 100 LIMIT 10", False),
        # UNION — not a bare SELECT
        ("SELECT a FROM x UNION SELECT b FROM y", False),
        # CTE — not exploratory
        ("WITH cte AS (SELECT 1) SELECT * FROM cte LIMIT 1", False),
        # GROUP BY without aggregate
        ("SELECT status FROM orders GROUP BY status LIMIT 5", False),
        # HAVING
        (
            "SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 1",
            False,
        ),
        # Aggregate without GROUP BY (scalar aggregate)
        ("SELECT COUNT(*) FROM orders", False),
        # SUM
        ("SELECT SUM(total) FROM orders", False),
        # Inner LIMIT only — outer SELECT has no LIMIT, so not exploratory
        ("SELECT * FROM (SELECT * FROM orders LIMIT 5) t", False),
    ],
)
def test_is_exploratory(sql, expected):
    assert is_exploratory(sql) is expected


def test_unparseable_sql_returns_false():
    assert is_exploratory("NOT VALID SQL $$$$") is False


def test_empty_string_returns_false():
    assert is_exploratory("") is False
