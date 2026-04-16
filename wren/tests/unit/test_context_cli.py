"""Integration tests for the `wren context` CLI sub-app."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from wren.cli import app

runner = CliRunner()


# ── Helpers ────────────────────────────────────────────────────────────────


def _make_valid_project(tmp_path: Path) -> Path:
    """Write a minimal valid v2 project in tmp_path."""
    (tmp_path / "wren_project.yml").write_text(
        'schema_version: 2\nname: test_proj\nversion: "1.0"\n'
        "catalog: wren\nschema: public\ndata_source: postgres\n"
    )
    model_dir = tmp_path / "models" / "orders"
    model_dir.mkdir(parents=True)
    (model_dir / "metadata.yml").write_text(
        "name: orders\n"
        'table_reference:\n  catalog: ""\n  schema: public\n  table: orders\n'
        "columns:\n"
        "  - name: id\n    type: INTEGER\n    is_calculated: false\n    not_null: true\n    properties: {}\n"
        "  - name: total\n    type: DECIMAL\n    is_calculated: false\n    not_null: false\n    properties: {}\n"
        "primary_key: id\ncached: false\nproperties:\n  description: Orders table\n"
    )
    view_dir = tmp_path / "views" / "summary"
    view_dir.mkdir(parents=True)
    (view_dir / "metadata.yml").write_text(
        "name: summary\nproperties:\n  description: test view\n"
    )
    (view_dir / "sql.yml").write_text(
        "statement: SELECT id, total FROM wren.public.orders\n"
    )
    (tmp_path / "relationships.yml").write_text("relationships: []\n")
    return tmp_path


# ── wren context init ─────────────────────────────────────────────────────


def test_init_creates_scaffold(tmp_path):
    result = runner.invoke(app, ["context", "init", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "wren_project.yml").exists()
    assert (tmp_path / "models" / "example" / "metadata.yml").exists()
    assert (tmp_path / "views" / "example_view" / "metadata.yml").exists()
    assert (tmp_path / "views" / "example_view" / "sql.yml").exists()
    assert (tmp_path / "relationships.yml").exists()
    assert (tmp_path / "instructions.md").exists()

    # Verify wren_project.yml contains namespace clarification comments and defaults
    project_yml = (tmp_path / "wren_project.yml").read_text()
    assert "catalog: wren" in project_yml
    assert "schema: public" in project_yml
    assert "data_source: postgres" in project_yml
    assert "NOT your database" in project_yml

    # Verify example model metadata contains table_reference annotation
    model_meta = (tmp_path / "models" / "example" / "metadata.yml").read_text()
    assert "table_reference" in model_meta
    assert "ACTUAL database" in model_meta


def test_init_refuses_existing(tmp_path):
    (tmp_path / "wren_project.yml").write_text("name: existing\n")
    result = runner.invoke(app, ["context", "init", "--path", str(tmp_path)])
    assert result.exit_code == 1
    assert "already exists" in result.output


# ── wren context validate ─────────────────────────────────────────────────


def test_validate_pass(tmp_path):
    _make_valid_project(tmp_path)
    result = runner.invoke(app, ["context", "validate", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "Valid" in result.output


def test_validate_fail(tmp_path):
    # Missing data_source in project config
    (tmp_path / "wren_project.yml").write_text("schema_version: 2\nname: broken\n")
    result = runner.invoke(app, ["context", "validate", "--path", str(tmp_path)])
    assert result.exit_code == 1
    assert "ERROR" in result.output


def test_validate_strict_warns(tmp_path):
    _make_valid_project(tmp_path)
    # Add a relationship with missing join_type (warning)
    (tmp_path / "relationships.yml").write_text(
        "relationships:\n  - name: r\n    models: [orders]\n    condition: a = b\n"
    )
    # Without --strict: exit 0
    result = runner.invoke(app, ["context", "validate", "--path", str(tmp_path)])
    assert result.exit_code == 0

    # With --strict: exit 1
    result = runner.invoke(
        app, ["context", "validate", "--path", str(tmp_path), "--strict"]
    )
    assert result.exit_code == 1


# ── wren context build ────────────────────────────────────────────────────


def test_build_creates_target(tmp_path):
    _make_valid_project(tmp_path)
    result = runner.invoke(app, ["context", "build", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output
    out_file = tmp_path / "target" / "mdl.json"
    assert out_file.exists()
    data = json.loads(out_file.read_text())
    assert data["catalog"] == "wren"
    assert data["models"][0]["tableReference"]["table"] == "orders"


def test_build_validation_error(tmp_path):
    # Model with both table_reference and ref_sql
    (tmp_path / "wren_project.yml").write_text(
        "schema_version: 2\nname: test\ndata_source: postgres\ncatalog: wren\nschema: public\n"
    )
    d = tmp_path / "models" / "bad"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text(
        "name: bad\ntable_reference:\n  table: t\nref_sql: SELECT 1\ncolumns: []\n"
    )
    result = runner.invoke(app, ["context", "build", "--path", str(tmp_path)])
    assert result.exit_code == 1
    assert "aborted" in result.output


def test_build_no_validate(tmp_path):
    # Model with neither tref nor ref_sql — would normally fail validation
    (tmp_path / "wren_project.yml").write_text(
        "schema_version: 2\nname: test\ndata_source: postgres\ncatalog: wren\nschema: public\n"
    )
    d = tmp_path / "models" / "empty_model"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: empty_model\ncolumns: []\n")
    result = runner.invoke(
        app, ["context", "build", "--path", str(tmp_path), "--no-validate"]
    )
    assert result.exit_code == 0
    out_file = tmp_path / "target" / "mdl.json"
    assert out_file.exists()


# ── wren context show ─────────────────────────────────────────────────────


def test_show_summary(tmp_path):
    _make_valid_project(tmp_path)
    result = runner.invoke(app, ["context", "show", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "test_proj" in result.output
    assert "orders" in result.output


def test_show_json(tmp_path):
    _make_valid_project(tmp_path)
    result = runner.invoke(
        app, ["context", "show", "--path", str(tmp_path), "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert "models" in data
    assert data["models"][0]["tableReference"]["table"] == "orders"


def test_show_yaml(tmp_path):
    _make_valid_project(tmp_path)
    result = runner.invoke(
        app, ["context", "show", "--path", str(tmp_path), "--output", "yaml"]
    )
    assert result.exit_code == 0, result.output
    import yaml  # noqa: PLC0415

    data = yaml.safe_load(result.output)
    assert "models" in data


# ── wren context instructions ─────────────────────────────────────────────


def test_instructions_prints_content(tmp_path):
    _make_valid_project(tmp_path)
    (tmp_path / "instructions.md").write_text("## Rule 1\nAlways use UTC.\n")
    result = runner.invoke(app, ["context", "instructions", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "Rule 1" in result.output
    assert "UTC" in result.output


def test_instructions_empty_when_missing(tmp_path):
    _make_valid_project(tmp_path)
    result = runner.invoke(app, ["context", "instructions", "--path", str(tmp_path)])
    assert result.exit_code == 0
    assert result.output.strip() == ""


def test_instructions_discovers_project(tmp_path):
    _make_valid_project(tmp_path)
    (tmp_path / "instructions.md").write_text("custom rule here")
    result = runner.invoke(app, ["context", "instructions", "--path", str(tmp_path)])
    assert result.exit_code == 0
    assert "custom rule here" in result.output


# ── wren context upgrade ─────────────────────────────────────────────────


def _make_v1_project(tmp_path: Path) -> Path:
    (tmp_path / "wren_project.yml").write_text(
        "schema_version: 1\nname: test\ndata_source: postgres\ncatalog: wren\nschema: public\n"
    )
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "orders.yml").write_text(
        "name: orders\ntable_reference:\n  table: orders\n"
        "columns:\n  - name: id\n    type: INTEGER\nprimary_key: id\n"
    )
    (tmp_path / "relationships.yml").write_text("relationships: []\n")
    return tmp_path


def test_upgrade_cli_v2_to_v3(tmp_path):
    _make_valid_project(tmp_path)
    result = runner.invoke(app, ["context", "upgrade", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "Upgrade complete" in result.output
    import yaml

    config = yaml.safe_load((tmp_path / "wren_project.yml").read_text())
    assert config["schema_version"] == 3


def test_upgrade_cli_dry_run(tmp_path):
    _make_v1_project(tmp_path)
    result = runner.invoke(
        app, ["context", "upgrade", "--path", str(tmp_path), "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "Dry run" in result.output
    assert "Would create" in result.output
    # Verify no files were actually changed
    assert not (tmp_path / "models" / "orders" / "metadata.yml").exists()
    import yaml

    config = yaml.safe_load((tmp_path / "wren_project.yml").read_text())
    assert config["schema_version"] == 1


def test_upgrade_cli_already_current(tmp_path):
    (tmp_path / "wren_project.yml").write_text(
        "schema_version: 3\nname: test\ndata_source: postgres\n"
    )
    result = runner.invoke(app, ["context", "upgrade", "--path", str(tmp_path)])
    assert result.exit_code == 0
    assert "Already at" in result.output


def test_upgrade_cli_explicit_to_version(tmp_path):
    _make_v1_project(tmp_path)
    result = runner.invoke(
        app, ["context", "upgrade", "--path", str(tmp_path), "--to", "2"]
    )
    assert result.exit_code == 0, result.output
    assert "Upgrade complete" in result.output
    import yaml

    config = yaml.safe_load((tmp_path / "wren_project.yml").read_text())
    assert config["schema_version"] == 2
