"""Unit tests for WrenEngine — no database required.

transpile() and dry_plan() exercise the wren-core MDL planning + sqlglot
transpile path without connecting to any data source.
"""

from __future__ import annotations

import base64
import math

import orjson
import pytest

from wren import WrenEngine
from wren.model.data_source import DataSource
from wren.model.error import WrenError

pytestmark = pytest.mark.unit

# Minimal manifest with a single model.  No real DB needed for planning.
_MANIFEST = {
    "catalog": "wren",
    "schema": "public",
    "models": [
        {
            "name": "orders",
            "tableReference": {"schema": "main", "table": "orders"},
            "columns": [
                {"name": "o_orderkey", "type": "integer"},
                {"name": "o_custkey", "type": "integer"},
                {"name": "o_orderstatus", "type": "varchar"},
                {
                    "name": "order_cust_key",
                    "type": "varchar",
                    "expression": "concat(cast(o_orderkey as varchar), '_', cast(o_custkey as varchar))",
                },
            ],
            "primaryKey": "o_orderkey",
        }
    ],
}
_MANIFEST_STR = base64.b64encode(orjson.dumps(_MANIFEST)).decode()


@pytest.fixture(scope="module")
def duckdb_engine(tmp_path_factory):
    """A WrenEngine pointed at a temporary DuckDB file (not queried by unit tests)."""
    db_dir = tmp_path_factory.mktemp("unit_duckdb")
    conn_info = {"url": str(db_dir), "format": "duckdb"}
    with WrenEngine(_MANIFEST_STR, DataSource.duckdb, conn_info) as e:
        yield e


@pytest.fixture(scope="module")
def pg_engine():
    """A WrenEngine configured for Postgres (no real connection opened for planning)."""
    conn_info = {
        "host": "localhost",
        "port": 5432,
        "database": "test",
        "user": "test",
        "password": "test",
    }
    with WrenEngine(_MANIFEST_STR, DataSource.postgres, conn_info) as e:
        yield e


# ------------------------------------------------------------------
# transpile
# ------------------------------------------------------------------


def test_transpile_returns_string(duckdb_engine: WrenEngine) -> None:
    sql = duckdb_engine.transpile('SELECT o_orderkey FROM "orders" LIMIT 1')
    assert isinstance(sql, str)
    assert len(sql) > 0


def test_transpile_postgres_dialect(pg_engine: WrenEngine) -> None:
    """Transpile should produce Postgres-flavoured SQL (no backtick quoting, etc.)."""
    sql = pg_engine.transpile('SELECT o_orderkey FROM "orders" LIMIT 1')
    assert isinstance(sql, str)
    # sqlglot Postgres output uses double-quote identifiers, not backticks
    assert "`" not in sql


def test_transpile_calculated_field(duckdb_engine: WrenEngine) -> None:
    sql = duckdb_engine.transpile('SELECT order_cust_key FROM "orders" LIMIT 1')
    assert isinstance(sql, str)
    # The calculated column expression should be expanded in the SQL
    assert "concat" in sql.lower() or "||" in sql.lower()


def test_transpile_invalid_sql_raises(duckdb_engine: WrenEngine) -> None:
    with pytest.raises(WrenError):
        duckdb_engine.transpile("SELECT * FROM not_a_model_in_manifest")


# ------------------------------------------------------------------
# dry_plan
# ------------------------------------------------------------------


def test_dry_plan_returns_datafusion_sql(duckdb_engine: WrenEngine) -> None:
    planned = duckdb_engine.dry_plan('SELECT o_orderkey FROM "orders" LIMIT 1')
    assert isinstance(planned, str)
    assert len(planned) > 0


def test_dry_plan_invalid_sql_raises(duckdb_engine: WrenEngine) -> None:
    with pytest.raises(WrenError):
        duckdb_engine.dry_plan("SELECT * FROM nonexistent_model")


# ------------------------------------------------------------------
# Context manager
# ------------------------------------------------------------------


def test_context_manager_closes_connector() -> None:
    conn_info = {"url": "/tmp", "format": "duckdb"}
    with WrenEngine(_MANIFEST_STR, DataSource.duckdb, conn_info) as e:
        assert e._connector is None  # connector is lazily initialized

    # After __exit__, internal state is cleaned up
    assert e._connector is None
