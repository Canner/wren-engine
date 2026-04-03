"""PostgreSQL connector tests.

Uses testcontainers to spin up a real Postgres instance.
TPCH data is generated via DuckDB's built-in extension and loaded via psycopg.
"""

from __future__ import annotations

import base64
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

pytestmark = pytest.mark.postgres

_SCHEMA = "public"


def _load_tpch(conn_str: str) -> None:
    """Generate TPCH sf=0.01 via DuckDB and bulk-load into Postgres."""
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
                    o_orderkey   INTEGER PRIMARY KEY,
                    o_custkey    INTEGER NOT NULL,
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


class TestPostgres(WrenQueryTestSuite):
    manifest = make_tpch_manifest(table_catalog=None, table_schema=_SCHEMA)

    @pytest.fixture(scope="class")
    def engine(self) -> WrenEngine:  # type: ignore[override]
        with PostgresContainer("postgres:16") as pg:
            # testcontainers returns a SQLAlchemy-style URL; psycopg wants
            # the plain postgresql:// form.
            url = pg.get_connection_url().replace("+psycopg2", "")
            _load_tpch(url)

            parsed = urlparse(url)
            conn_info = {
                "host": parsed.hostname,
                "port": parsed.port,
                "database": parsed.path.lstrip("/"),
                "user": parsed.username,
                "password": parsed.password,
            }
            manifest_str = base64.b64encode(orjson.dumps(self.manifest)).decode()
            with WrenEngine(
                manifest_str, DataSource.postgres, conn_info, fallback=False
            ) as e:
                yield e
