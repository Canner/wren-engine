"""Redshift connector tests.

Uses testcontainers to spin up a PostgreSQL instance as a Redshift-compatible
backend (Redshift is based on PostgreSQL and shares the same SQL dialect).

Two known limitations apply when testing against vanilla PostgreSQL:

1. redshift_connector sends Redshift-specific startup parameters
   (client_protocol_version) that PostgreSQL rejects with FATAL.  The
   driver is therefore patched to use psycopg so tests can verify the
   full connector/query path against a real database.

2. The Redshift SQL dialect emits VARCHAR(MAX) for any string-cast
   expression (e.g. CAST(x AS TEXT) or CAST(x AS VARCHAR)).  PostgreSQL
   does not support VARCHAR(MAX), so tests that expand the full model and
   include the order_cust_key calculated field are skipped.
"""

from __future__ import annotations

import base64
from unittest.mock import patch
from urllib.parse import urlparse

import duckdb
import orjson
import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from tests.suite.manifests import make_tpch_manifest
from tests.suite.query import WrenQueryTestSuite
from wren import WrenEngine
from wren.model.data_source import DataSource

pytestmark = pytest.mark.redshift

_SCHEMA = "public"
_SKIP_VARCHAR_MAX = pytest.mark.skip(
    reason=(
        "Redshift dialect emits VARCHAR(MAX) for string casts; "
        "not supported by the PostgreSQL test backend"
    )
)


def _load_tpch(conn_str: str) -> None:
    """Generate TPCH sf=0.01 via DuckDB and bulk-load into PostgreSQL."""
    duck = duckdb.connect()
    duck.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01)")
    orders_rows = duck.execute(
        "SELECT o_orderkey, o_custkey, o_orderstatus, "
        "cast(o_totalprice as double), o_orderdate FROM orders"
    ).fetchall()
    customer_rows = duck.execute("SELECT c_custkey, c_name FROM customer").fetchall()
    duck.close()

    with psycopg.connect(conn_str) as pg:
        with pg.cursor() as cur:
            cur.execute("""
                CREATE TABLE orders (
                    o_orderkey    INTEGER PRIMARY KEY,
                    o_custkey     INTEGER NOT NULL,
                    o_orderstatus CHAR(1) NOT NULL,
                    o_totalprice  DOUBLE PRECISION NOT NULL,
                    o_orderdate   DATE NOT NULL
                )
            """)
            cur.executemany(
                "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_rows
            )
            cur.execute("""
                CREATE TABLE customer (
                    c_custkey INTEGER PRIMARY KEY,
                    c_name    VARCHAR(25) NOT NULL
                )
            """)
            cur.executemany("INSERT INTO customer VALUES (%s, %s)", customer_rows)


class TestRedshift(WrenQueryTestSuite):
    manifest = make_tpch_manifest(table_catalog=None, table_schema=_SCHEMA)
    order_id_dtype = "int64"  # pandas default for Python int from PostgreSQL INTEGER

    # --- tests skipped due to VARCHAR(MAX) dialect limitation ---

    @_SKIP_VARCHAR_MAX
    def test_count(self, engine: WrenEngine) -> None:
        super().test_count(engine)

    @_SKIP_VARCHAR_MAX
    def test_calculated_field(self, engine: WrenEngine) -> None:
        super().test_calculated_field(engine)

    @_SKIP_VARCHAR_MAX
    def test_dry_run_valid(self, engine: WrenEngine) -> None:
        super().test_dry_run_valid(engine)

    @pytest.fixture(scope="class")
    def engine(self) -> WrenEngine:  # type: ignore[override]
        with PostgresContainer("postgres:16") as pg:
            url = pg.get_connection_url().replace("+psycopg2", "")
            _load_tpch(url)

            parsed = urlparse(url)

            def _psycopg_connect(**kwargs):
                """Proxy redshift_connector.connect() → psycopg.

                redshift_connector sends Redshift-specific startup params
                (e.g. client_protocol_version) that standard PostgreSQL
                rejects with FATAL.  We redirect to psycopg so tests can
                verify the full connector/query path against a real database.
                """
                conn = psycopg.connect(
                    host=kwargs["host"],
                    port=int(kwargs["port"]),
                    dbname=kwargs["database"],
                    user=kwargs["user"],
                    password=kwargs.get("password", ""),
                )
                conn.autocommit = True
                return conn

            conn_info = {
                "host": parsed.hostname,
                "port": parsed.port,
                "database": parsed.path.lstrip("/"),
                "user": parsed.username,
                "password": parsed.password or "",
                "ssl": False,
            }
            manifest_str = base64.b64encode(orjson.dumps(self.manifest)).decode()
            with patch("redshift_connector.connect", side_effect=_psycopg_connect):
                with WrenEngine(manifest_str, DataSource.redshift, conn_info) as e:
                    yield e
