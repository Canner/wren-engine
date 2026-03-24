"""BigQuery connector tests using the goccy/bigquery-emulator Docker image.

The emulator exposes the BigQuery REST API locally.  We point the
google-cloud-bigquery client at it via BIGQUERY_EMULATOR_HOST and load
TPCH data using streaming inserts before running the shared test suite.
"""

from __future__ import annotations

import base64
import json
import os

import duckdb
import orjson
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.suite.manifests import make_tpch_manifest
from tests.suite.query import WrenQueryTestSuite
from wren import WrenEngine
from wren.model.data_source import DataSource

pytestmark = pytest.mark.bigquery

_PROJECT = "test"
_DATASET = "test"
_EMULATOR_PORT = 9050
_BATCH_SIZE = 500


def _make_fake_credentials() -> str:
    """Return a base64-encoded fake service account JSON with a real RSA key.

    google-auth validates the private key format locally; the emulator never
    checks the actual credentials when BIGQUERY_EMULATOR_HOST is set.
    """
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    sa = {
        "type": "service_account",
        "project_id": _PROJECT,
        "private_key_id": "key1",
        "private_key": private_pem,
        "client_email": f"wren@{_PROJECT}.iam.gserviceaccount.com",
        "client_id": "123456789",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    return base64.b64encode(json.dumps(sa).encode()).decode()


def _load_tpch(host: str, port: int) -> None:
    """Generate TPCH sf=0.01 via DuckDB and load into the BigQuery emulator."""
    import google.auth.credentials  # noqa: PLC0415
    from google.api_core.client_options import ClientOptions  # noqa: PLC0415
    from google.cloud import bigquery  # noqa: PLC0415

    duck = duckdb.connect()
    duck.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01)")
    orders_rows = duck.execute(
        "SELECT o_orderkey, o_custkey, o_orderstatus, "
        "cast(o_totalprice as double), cast(o_orderdate as varchar) FROM orders"
    ).fetchall()
    customer_rows = duck.execute("SELECT c_custkey, c_name FROM customer").fetchall()
    duck.close()

    client = bigquery.Client(
        project=_PROJECT,
        credentials=google.auth.credentials.AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"http://{host}:{port}"),
    )

    dataset_ref = bigquery.DatasetReference(_PROJECT, _DATASET)
    orders_table = bigquery.Table(
        dataset_ref.table("orders"),
        schema=[
            bigquery.SchemaField("o_orderkey", "INTEGER"),
            bigquery.SchemaField("o_custkey", "INTEGER"),
            bigquery.SchemaField("o_orderstatus", "STRING"),
            bigquery.SchemaField("o_totalprice", "FLOAT64"),
            bigquery.SchemaField("o_orderdate", "DATE"),
        ],
    )
    customer_table = bigquery.Table(
        dataset_ref.table("customer"),
        schema=[
            bigquery.SchemaField("c_custkey", "INTEGER"),
            bigquery.SchemaField("c_name", "STRING"),
        ],
    )
    client.create_table(orders_table, exists_ok=True)
    client.create_table(customer_table, exists_ok=True)

    orders_json = [
        {
            "o_orderkey": r[0],
            "o_custkey": r[1],
            "o_orderstatus": r[2],
            "o_totalprice": r[3],
            "o_orderdate": r[4],
        }
        for r in orders_rows
    ]
    customer_json = [{"c_custkey": r[0], "c_name": r[1]} for r in customer_rows]

    for i in range(0, len(orders_json), _BATCH_SIZE):
        errors = client.insert_rows_json(orders_table, orders_json[i : i + _BATCH_SIZE])
        if errors:
            raise RuntimeError(f"BigQuery insert errors (orders batch {i}): {errors}")

    for i in range(0, len(customer_json), _BATCH_SIZE):
        errors = client.insert_rows_json(
            customer_table, customer_json[i : i + _BATCH_SIZE]
        )
        if errors:
            raise RuntimeError(f"BigQuery insert errors (customer batch {i}): {errors}")


class TestBigQuery(WrenQueryTestSuite):
    manifest = make_tpch_manifest(table_catalog=_PROJECT, table_schema=_DATASET)
    order_id_dtype = "int64"  # BigQuery INTEGER → Arrow int64

    @pytest.fixture(scope="class")
    def engine(self) -> WrenEngine:  # type: ignore[override]
        container = (
            DockerContainer("ghcr.io/goccy/bigquery-emulator:latest")
            .with_command(f"--project={_PROJECT} --dataset={_DATASET}")
            .with_exposed_ports(_EMULATOR_PORT)
            .with_kwargs(platform="linux/amd64")  # emulator only publishes amd64
        )
        with container as bq:
            wait_for_logs(bq, "REST server listening at", timeout=60)
            host = bq.get_container_host_ip()
            port = int(bq.get_exposed_port(_EMULATOR_PORT))
            os.environ["BIGQUERY_EMULATOR_HOST"] = f"{host}:{port}"
            try:
                _load_tpch(host, port)
                conn_info = {
                    "project_id": _PROJECT,
                    "dataset_id": _DATASET,
                    "credentials": _make_fake_credentials(),
                }
                manifest_str = base64.b64encode(orjson.dumps(self.manifest)).decode()
                with WrenEngine(manifest_str, DataSource.bigquery, conn_info) as e:
                    yield e
            finally:
                os.environ.pop("BIGQUERY_EMULATOR_HOST", None)
