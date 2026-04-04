"""Unit tests for wren.context.validate — no database required."""

from __future__ import annotations

import base64
import json

import orjson
import pytest
from typer.testing import CliRunner

from wren.context import validate
from wren.model.data_source import DataSource

pytestmark = pytest.mark.unit

# ── Shared manifests ──────────────────────────────────────────────────────


def _b64(manifest: dict) -> str:
    return base64.b64encode(orjson.dumps(manifest)).decode()


_MODEL_WITH_DESC = {
    "name": "orders",
    "tableReference": {"schema": "main", "table": "orders"},
    "columns": [
        {"name": "o_orderkey", "type": "integer"},
        {"name": "o_custkey", "type": "integer"},
    ],
    "primaryKey": "o_orderkey",
    "properties": {"description": "Orders model"},
}

_MODEL_WITHOUT_DESC = {
    "name": "accounts",
    "tableReference": {"schema": "main", "table": "accounts"},
    "columns": [
        {"name": "acct_id", "type": "integer"},
        {"name": "plan_cd", "type": "varchar"},
    ],
    "primaryKey": "acct_id",
    # no properties.description
}

_VALID_VIEW = {
    "name": "valid_view",
    "statement": 'SELECT o_orderkey FROM "orders"',
    "properties": {"description": "A valid view"},
}

_VIEW_WITHOUT_DESC = {
    "name": "daily_usage",
    "statement": 'SELECT o_orderkey FROM "orders"',
    # no description
}

_BROKEN_VIEW = {
    "name": "stale_report",
    "statement": 'SELECT * FROM "deleted_model"',
    # deleted_model does not exist in the manifest
}

_EMPTY_STMT_VIEW = {
    "name": "empty_view",
    "statement": "",
}

_BASE_MANIFEST = {
    "catalog": "wren",
    "schema": "public",
    "models": [_MODEL_WITH_DESC],
}

# ── View dry-plan tests ───────────────────────────────────────────────────


def test_validate_view_dry_plan_pass():
    manifest = {**_BASE_MANIFEST, "views": [_VALID_VIEW]}
    result = validate(_b64(manifest), DataSource.duckdb)
    assert result["errors"] == []


def test_validate_view_dry_plan_error():
    manifest = {**_BASE_MANIFEST, "views": [_BROKEN_VIEW]}
    result = validate(_b64(manifest), DataSource.duckdb)
    assert len(result["errors"]) == 1
    assert "stale_report" in result["errors"][0]
    assert "dry-plan failed" in result["errors"][0]


def test_validate_view_empty_statement():
    manifest = {**_BASE_MANIFEST, "views": [_EMPTY_STMT_VIEW]}
    result = validate(_b64(manifest), DataSource.duckdb)
    assert len(result["errors"]) == 1
    assert "empty_view" in result["errors"][0]
    assert "empty statement" in result["errors"][0]


# ── Description warning tests ─────────────────────────────────────────────


def test_validate_model_no_description_warning():
    manifest = {
        "catalog": "wren",
        "schema": "public",
        "models": [_MODEL_WITHOUT_DESC],
    }
    result = validate(_b64(manifest), DataSource.duckdb)
    assert result["errors"] == []
    assert any("accounts" in w for w in result["warnings"])


def test_validate_view_no_description_warning():
    manifest = {**_BASE_MANIFEST, "views": [_VIEW_WITHOUT_DESC]}
    result = validate(_b64(manifest), DataSource.duckdb)
    assert result["errors"] == []
    assert any("daily_usage" in w for w in result["warnings"])


def test_validate_level_error_suppresses_warnings():
    manifest = {
        "catalog": "wren",
        "schema": "public",
        "models": [_MODEL_WITHOUT_DESC],
    }
    result = validate(_b64(manifest), DataSource.duckdb, level="error")
    assert result["errors"] == []
    assert result["warnings"] == []


def test_validate_strict_column_warning():
    manifest = {
        "catalog": "wren",
        "schema": "public",
        "models": [_MODEL_WITHOUT_DESC],
    }
    result = validate(_b64(manifest), DataSource.duckdb, level="strict")
    assert result["errors"] == []
    # Should warn on model (no desc) AND each column (no desc)
    warning_text = " ".join(result["warnings"])
    assert "accounts" in warning_text
    assert "plan_cd" in warning_text
    assert "acct_id" in warning_text


# ── CLI exit-code tests ───────────────────────────────────────────────────


@pytest.fixture()
def mdl_with_broken_view(tmp_path):
    manifest = {**_BASE_MANIFEST, "views": [_BROKEN_VIEW]}
    path = tmp_path / "mdl.json"
    path.write_text(json.dumps(manifest))
    return str(path)


@pytest.fixture()
def mdl_with_no_desc(tmp_path):
    manifest = {
        "catalog": "wren",
        "schema": "public",
        "models": [_MODEL_WITHOUT_DESC],
    }
    path = tmp_path / "mdl.json"
    path.write_text(json.dumps(manifest))
    return str(path)


def test_validate_errors_exit_1(mdl_with_broken_view):
    from wren.cli import app  # noqa: PLC0415

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "context",
            "validate",
            "--mdl",
            mdl_with_broken_view,
            "--datasource",
            "duckdb",
        ],
    )
    assert result.exit_code == 1
    assert "stale_report" in result.output


def test_validate_warnings_exit_0(mdl_with_no_desc):
    from wren.cli import app  # noqa: PLC0415

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["context", "validate", "--mdl", mdl_with_no_desc, "--datasource", "duckdb"],
    )
    assert result.exit_code == 0
    assert "accounts" in result.output
    assert "\u2713" in result.output  # ✓
