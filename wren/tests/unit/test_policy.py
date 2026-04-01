"""Unit tests for wren.policy — SQL policy validation.

These tests use sqlglot parsing only and do not require a database or wren-core.
"""

from __future__ import annotations

import pytest
from sqlglot import parse_one

from wren.config import WrenConfig
from wren.model.error import ErrorCode, WrenError
from wren.policy import validate_sql_policy

pytestmark = pytest.mark.unit

_MODELS = {"orders", "customers"}


# ── Table validation ──────────────────────────────────────────────────────


def test_valid_query_all_tables_in_mdl():
    ast = parse_one('SELECT * FROM "orders"', dialect="duckdb")
    config = WrenConfig(strict_mode=True)
    validate_sql_policy(ast, _MODELS, set(), config)


def test_table_not_in_mdl_raises():
    ast = parse_one("SELECT * FROM pg_shadow", dialect="postgres")
    config = WrenConfig(strict_mode=True)
    with pytest.raises(WrenError) as exc_info:
        validate_sql_policy(ast, _MODELS, set(), config)
    assert exc_info.value.error_code == ErrorCode.MODEL_NOT_FOUND
    assert "pg_shadow" in str(exc_info.value)


def test_user_cte_not_flagged():
    sql = "WITH foo AS (SELECT 1 AS x) SELECT * FROM foo"
    ast = parse_one(sql, dialect="duckdb")
    config = WrenConfig(strict_mode=True)
    validate_sql_policy(ast, _MODELS, {"foo"}, config)


def test_mixed_mdl_and_non_mdl_table_raises():
    sql = 'SELECT * FROM "orders" JOIN secret_table ON 1=1'
    ast = parse_one(sql, dialect="duckdb")
    config = WrenConfig(strict_mode=True)
    with pytest.raises(WrenError) as exc_info:
        validate_sql_policy(ast, _MODELS, set(), config)
    assert exc_info.value.error_code == ErrorCode.MODEL_NOT_FOUND
    assert "secret_table" in str(exc_info.value)


def test_strict_mode_off_allows_unknown_table():
    ast = parse_one("SELECT * FROM unknown_table", dialect="duckdb")
    config = WrenConfig(strict_mode=False)
    validate_sql_policy(ast, _MODELS, set(), config)


def test_subquery_alias_not_flagged():
    sql = "SELECT * FROM (SELECT 1 AS x) AS t"
    ast = parse_one(sql, dialect="duckdb")
    config = WrenConfig(strict_mode=True)
    # 't' is a subquery alias, not a real table — but sqlglot doesn't emit
    # an exp.Table for it, so this should pass without error.
    # The only table nodes come from real FROM references.
    validate_sql_policy(ast, _MODELS, set(), config)


def test_multiple_valid_tables():
    sql = 'SELECT * FROM "orders" o JOIN "customers" c ON o.id = c.id'
    ast = parse_one(sql, dialect="duckdb")
    config = WrenConfig(strict_mode=True)
    validate_sql_policy(ast, _MODELS, set(), config)


# ── Denied functions ──────────────────────────────────────────────────────


def test_denied_function_raises():
    ast = parse_one("SELECT pg_read_file('/etc/passwd')", dialect="postgres")
    config = WrenConfig(denied_functions=frozenset(["pg_read_file"]))
    with pytest.raises(WrenError) as exc_info:
        validate_sql_policy(ast, _MODELS, set(), config)
    assert exc_info.value.error_code == ErrorCode.BLOCKED_FUNCTION
    assert "pg_read_file" in str(exc_info.value)


def test_denied_function_case_insensitive():
    ast = parse_one("SELECT PG_READ_FILE('/etc/passwd')", dialect="postgres")
    config = WrenConfig(denied_functions=frozenset(["pg_read_file"]))
    with pytest.raises(WrenError) as exc_info:
        validate_sql_policy(ast, _MODELS, set(), config)
    assert exc_info.value.error_code == ErrorCode.BLOCKED_FUNCTION


def test_allowed_function_passes():
    ast = parse_one('SELECT COUNT(*) FROM "orders"', dialect="duckdb")
    config = WrenConfig(denied_functions=frozenset(["pg_read_file"]))
    validate_sql_policy(ast, _MODELS, set(), config)


def test_builtin_function_on_denied_list():
    ast = parse_one('SELECT COUNT(*) FROM "orders"', dialect="duckdb")
    config = WrenConfig(denied_functions=frozenset(["count"]))
    with pytest.raises(WrenError) as exc_info:
        validate_sql_policy(ast, _MODELS, set(), config)
    assert exc_info.value.error_code == ErrorCode.BLOCKED_FUNCTION


def test_nested_denied_function():
    sql = "SELECT * FROM (SELECT dblink('host=evil', 'SELECT 1') AS x) AS t"
    ast = parse_one(sql, dialect="postgres")
    config = WrenConfig(denied_functions=frozenset(["dblink"]))
    with pytest.raises(WrenError) as exc_info:
        validate_sql_policy(ast, _MODELS, set(), config)
    assert exc_info.value.error_code == ErrorCode.BLOCKED_FUNCTION


def test_no_denied_list_allows_everything():
    ast = parse_one("SELECT pg_read_file('/etc/passwd')", dialect="postgres")
    config = WrenConfig(denied_functions=frozenset())
    validate_sql_policy(ast, _MODELS, set(), config)


def test_empty_denied_list_allows_everything():
    ast = parse_one("SELECT dblink('host=evil', 'SELECT 1')", dialect="postgres")
    config = WrenConfig()
    validate_sql_policy(ast, _MODELS, set(), config)


# ── Combined strict_mode + denied_functions ───────────────────────────────


def test_strict_mode_and_denied_functions_together():
    sql = 'SELECT pg_read_file(o_orderkey) FROM "orders"'
    ast = parse_one(sql, dialect="postgres")
    config = WrenConfig(
        strict_mode=True, denied_functions=frozenset(["pg_read_file"])
    )
    with pytest.raises(WrenError) as exc_info:
        validate_sql_policy(ast, _MODELS, set(), config)
    # Either error is acceptable — table check runs first, but orders is valid
    # so function check should fire
    assert exc_info.value.error_code == ErrorCode.BLOCKED_FUNCTION
