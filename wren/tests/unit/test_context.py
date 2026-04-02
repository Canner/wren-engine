"""Unit tests for wren.context — load/validate/build YAML→JSON."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from wren.context import (
    ValidationError,
    _convert_keys,
    _snake_to_camel,
    build_json,
    build_manifest,
    discover_project_path,
    get_schema_version,
    load_instructions,
    load_models,
    load_relationships,
    load_views,
    require_schema_version,
    save_target,
    validate_project,
)


# ── Case conversion ────────────────────────────────────────────────────────


def test_snake_to_camel():
    assert _snake_to_camel("table_reference") == "tableReference"
    assert _snake_to_camel("is_primary_key") == "isPrimaryKey"
    assert _snake_to_camel("ref_sql") == "refSql"
    assert _snake_to_camel("join_type") == "joinType"
    assert _snake_to_camel("not_null") == "notNull"
    assert _snake_to_camel("is_calculated") == "isCalculated"
    assert _snake_to_camel("primary_key") == "primaryKey"
    assert _snake_to_camel("data_source") == "dataSource"
    assert _snake_to_camel("name") == "name"


def test_convert_keys_nested():
    obj = {
        "table_reference": {"catalog": "c", "schema_name": "s"},
        "columns": [{"is_calculated": False, "not_null": True}],
    }
    result = _convert_keys(obj)
    assert "tableReference" in result
    assert "schemaName" in result["tableReference"]
    assert result["columns"][0]["isCalculated"] is False
    assert result["columns"][0]["notNull"] is True


# ── Schema version ─────────────────────────────────────────────────────────


def test_get_schema_version_default(tmp_path):
    (tmp_path / "wren_project.yml").write_text("name: test\ndata_source: postgres\n")
    assert get_schema_version(tmp_path) == 1


def test_get_schema_version_explicit(tmp_path):
    (tmp_path / "wren_project.yml").write_text("schema_version: 2\nname: test\ndata_source: postgres\n")
    assert get_schema_version(tmp_path) == 2


def test_require_schema_version_unsupported(tmp_path):
    (tmp_path / "wren_project.yml").write_text("schema_version: 99\nname: test\ndata_source: postgres\n")
    with pytest.raises(SystemExit, match="unsupported schema_version"):
        require_schema_version(tmp_path)


# ── load_models (v2) ──────────────────────────────────────────────────────


def _make_v2_project(tmp_path: Path, schema_version: int = 2) -> Path:
    """Write wren_project.yml with the given schema_version."""
    (tmp_path / "wren_project.yml").write_text(
        f"schema_version: {schema_version}\nname: test\ndata_source: postgres\ncatalog: wren\nschema: public\n"
    )
    return tmp_path


def test_load_models_from_dirs(tmp_path):
    _make_v2_project(tmp_path)
    model_dir = tmp_path / "models" / "orders"
    model_dir.mkdir(parents=True)
    (model_dir / "metadata.yml").write_text(
        "name: orders\ntable_reference:\n  table: orders\ncolumns: []\n"
    )
    models = load_models(tmp_path)
    assert len(models) == 1
    assert models[0]["name"] == "orders"


def test_load_models_sorted(tmp_path):
    _make_v2_project(tmp_path)
    for name in ("zebra", "apple", "mango"):
        d = tmp_path / "models" / name
        d.mkdir(parents=True)
        (d / "metadata.yml").write_text(f"name: {name}\ntable_reference:\n  table: {name}\n")
    models = load_models(tmp_path)
    names = [m["name"] for m in models]
    assert names == sorted(names)


def test_load_models_ref_sql_file(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "revenue"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: revenue\ncolumns: []\n")
    (d / "ref_sql.sql").write_text("SELECT 1 AS x")
    models = load_models(tmp_path)
    assert models[0]["ref_sql"] == "SELECT 1 AS x"


def test_load_models_ref_sql_file_precedence(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "revenue"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: revenue\nref_sql: SELECT 0\ncolumns: []\n")
    (d / "ref_sql.sql").write_text("SELECT 1 AS x")
    models = load_models(tmp_path)
    assert models[0]["ref_sql"] == "SELECT 1 AS x"


def test_load_models_inline_ref_sql(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "active"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: active\nref_sql: SELECT DISTINCT id FROM orders\ncolumns: []\n")
    models = load_models(tmp_path)
    assert models[0]["ref_sql"] == "SELECT DISTINCT id FROM orders"


def test_load_models_table_reference(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "orders"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text(
        "name: orders\ntable_reference:\n  catalog: \"\"\n  schema: public\n  table: orders\ncolumns: []\n"
    )
    models = load_models(tmp_path)
    assert models[0]["table_reference"]["table"] == "orders"


def test_load_models_skips_non_dir(tmp_path):
    _make_v2_project(tmp_path)
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "stray.yml").write_text("name: stray\n")
    models = load_models(tmp_path)
    assert models == []


def test_load_models_skips_missing_metadata(tmp_path):
    _make_v2_project(tmp_path)
    (tmp_path / "models" / "empty_dir").mkdir(parents=True)
    models = load_models(tmp_path)
    assert models == []


# ── load_views (v2) ───────────────────────────────────────────────────────


def test_load_views_from_dirs(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "views" / "monthly"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: monthly\nstatement: SELECT 1\n")
    views = load_views(tmp_path)
    assert len(views) == 1
    assert views[0]["name"] == "monthly"


def test_load_views_sql_yml_precedence(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "views" / "monthly"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: monthly\nstatement: SELECT 0\n")
    (d / "sql.yml").write_text("statement: SELECT 1\n")
    views = load_views(tmp_path)
    assert views[0]["statement"].strip() == "SELECT 1"


def test_load_views_inline_statement(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "views" / "top"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: top\nstatement: SELECT * FROM orders LIMIT 10\n")
    views = load_views(tmp_path)
    assert "SELECT" in views[0]["statement"]


def test_load_views_skips_non_dir(tmp_path):
    _make_v2_project(tmp_path)
    views_dir = tmp_path / "views"
    views_dir.mkdir()
    (views_dir / "stray.yml").write_text("name: stray\n")
    views = load_views(tmp_path)
    assert views == []


def test_load_views_skips_missing_metadata(tmp_path):
    _make_v2_project(tmp_path)
    (tmp_path / "views" / "empty_dir").mkdir(parents=True)
    views = load_views(tmp_path)
    assert views == []


# ── load_models / load_views (v1) ─────────────────────────────────────────


def test_load_models_v1_flat_files(tmp_path):
    _make_v2_project(tmp_path, schema_version=1)
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "orders.yml").write_text("name: orders\ntable_reference:\n  table: orders\n")
    models = load_models(tmp_path)
    assert len(models) == 1
    assert models[0]["name"] == "orders"


def test_load_views_v1_single_file(tmp_path):
    _make_v2_project(tmp_path, schema_version=1)
    (tmp_path / "views.yml").write_text(
        "views:\n  - name: v1\n    statement: SELECT 1\n"
    )
    views = load_views(tmp_path)
    assert len(views) == 1
    assert views[0]["name"] == "v1"


# ── load_relationships ────────────────────────────────────────────────────


def test_load_relationships(tmp_path):
    _make_v2_project(tmp_path)
    (tmp_path / "relationships.yml").write_text(
        "relationships:\n"
        "  - name: orders_customers\n"
        "    models: [orders, customers]\n"
        "    join_type: MANY_TO_ONE\n"
        "    condition: orders.customer_id = customers.customer_id\n"
    )
    rels = load_relationships(tmp_path)
    assert len(rels) == 1
    assert rels[0]["name"] == "orders_customers"


# ── load_instructions ─────────────────────────────────────────────────────


def test_load_instructions(tmp_path):
    _make_v2_project(tmp_path)
    (tmp_path / "instructions.md").write_text("## Rule 1\nAlways use snake_case.\n")
    result = load_instructions(tmp_path)
    assert result is not None
    assert "Rule 1" in result


def test_load_instructions_missing(tmp_path):
    _make_v2_project(tmp_path)
    assert load_instructions(tmp_path) is None


# ── build_manifest / build_json ───────────────────────────────────────────


def _minimal_v2_project(tmp_path: Path) -> Path:
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "orders"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text(
        "name: orders\n"
        "table_reference:\n  catalog: \"\"\n  schema: public\n  table: orders\n"
        "columns:\n  - name: id\n    type: INTEGER\n    is_calculated: false\n    not_null: true\n    is_primary_key: true\n    properties: {}\n"
        "primary_key: id\ncached: false\nproperties: {}\n"
    )
    return tmp_path


def test_build_manifest_snake_case(tmp_path):
    _minimal_v2_project(tmp_path)
    manifest = build_manifest(tmp_path)
    model = manifest["models"][0]
    assert "table_reference" in model
    assert "is_calculated" in model["columns"][0]
    assert "primary_key" in model


def test_build_json_camel_case(tmp_path):
    _minimal_v2_project(tmp_path)
    result = build_json(tmp_path)
    model = result["models"][0]
    assert "tableReference" in model
    assert "isCalculated" in model["columns"][0]
    assert "primaryKey" in model


def test_build_json_round_trip(tmp_path):
    _minimal_v2_project(tmp_path)
    result = build_json(tmp_path)
    serialized = json.dumps(result)
    parsed = json.loads(serialized)
    assert parsed["models"][0]["tableReference"]["table"] == "orders"
    assert parsed["models"][0]["primaryKey"] == "id"


# ── save_target ───────────────────────────────────────────────────────────


def test_save_target_creates_dir(tmp_path):
    _make_v2_project(tmp_path)
    manifest = {"catalog": "wren", "schema": "public", "models": []}
    out = save_target(manifest, tmp_path)
    assert out.exists()
    assert out.name == "mdl.json"
    loaded = json.loads(out.read_text())
    assert loaded["catalog"] == "wren"


# ── validate_project ──────────────────────────────────────────────────────


def _make_valid_project(tmp_path: Path) -> Path:
    """Build a minimal valid v2 project."""
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "orders"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text(
        "name: orders\n"
        "table_reference:\n  table: orders\n"
        "columns:\n  - name: id\n    type: INTEGER\n"
        "primary_key: id\n"
    )
    (tmp_path / "relationships.yml").write_text("relationships: []\n")
    return tmp_path


def test_validate_valid_project(tmp_path):
    _make_valid_project(tmp_path)
    errors = validate_project(tmp_path)
    assert errors == []


def test_validate_missing_project_yml(tmp_path):
    errors = validate_project(tmp_path)
    hard = [e for e in errors if e.level == "error"]
    assert any("not found" in e.message for e in hard)


def test_validate_missing_data_source(tmp_path):
    (tmp_path / "wren_project.yml").write_text("schema_version: 2\nname: test\n")
    errors = validate_project(tmp_path)
    assert any("data_source" in e.message for e in errors)


def test_validate_unsupported_schema_version(tmp_path):
    (tmp_path / "wren_project.yml").write_text(
        "schema_version: 99\nname: test\ndata_source: postgres\n"
    )
    errors = validate_project(tmp_path)
    assert any("unsupported schema_version" in e.message for e in errors)


def test_validate_missing_model_name(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "noname"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("table_reference:\n  table: t\ncolumns: []\n")
    errors = validate_project(tmp_path)
    assert any("missing 'name'" in e.message for e in errors)


def test_validate_duplicate_model(tmp_path):
    _make_v2_project(tmp_path)
    for folder in ("a", "b"):
        d = tmp_path / "models" / folder
        d.mkdir(parents=True)
        (d / "metadata.yml").write_text(
            "name: orders\ntable_reference:\n  table: orders\ncolumns: []\n"
        )
    errors = validate_project(tmp_path)
    assert any("duplicate model name" in e.message for e in errors)


def test_validate_both_tref_and_ref_sql(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "conflict"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text(
        "name: conflict\n"
        "table_reference:\n  table: t\n"
        "ref_sql: SELECT 1\n"
        "columns: []\n"
    )
    errors = validate_project(tmp_path)
    assert any("both 'table_reference' and 'ref_sql'" in e.message for e in errors)


def test_validate_neither_tref_nor_ref_sql(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "empty"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: empty\ncolumns: []\n")
    errors = validate_project(tmp_path)
    assert any("must define either" in e.message for e in errors)


def test_validate_pk_not_in_columns(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "models" / "orders"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text(
        "name: orders\n"
        "table_reference:\n  table: orders\n"
        "columns:\n  - name: id\n    type: INTEGER\n"
        "primary_key: missing_col\n"
    )
    errors = validate_project(tmp_path)
    assert any("not found in columns" in e.message for e in errors)


def test_validate_relationship_unknown_model(tmp_path):
    _make_valid_project(tmp_path)
    (tmp_path / "relationships.yml").write_text(
        "relationships:\n"
        "  - name: bad_rel\n"
        "    models: [orders, nonexistent]\n"
        "    join_type: MANY_TO_ONE\n"
        "    condition: a = b\n"
    )
    errors = validate_project(tmp_path)
    assert any("unknown model" in e.message for e in errors)


def test_validate_view_no_statement(tmp_path):
    _make_v2_project(tmp_path)
    d = tmp_path / "views" / "nostatement"
    d.mkdir(parents=True)
    (d / "metadata.yml").write_text("name: nostatement\ndescription: bad\n")
    errors = validate_project(tmp_path)
    assert any("missing 'statement'" in e.message for e in errors)


def test_validate_missing_join_type(tmp_path):
    _make_valid_project(tmp_path)
    (tmp_path / "relationships.yml").write_text(
        "relationships:\n"
        "  - name: r\n"
        "    models: [orders]\n"
        "    condition: a = b\n"
    )
    errors = validate_project(tmp_path)
    warnings = [e for e in errors if e.level == "warning"]
    assert any("join_type" in e.message for e in warnings)


# ── discover_project_path ─────────────────────────────────────────────────


def test_discover_walk_up(tmp_path, monkeypatch):
    # project file in parent; cwd is a subdir
    subdir = tmp_path / "sub" / "deep"
    subdir.mkdir(parents=True)
    (tmp_path / "wren_project.yml").write_text("name: test\ndata_source: pg\n")
    monkeypatch.chdir(subdir)
    result = discover_project_path()
    assert result == tmp_path


def test_discover_fallback(tmp_path, monkeypatch):
    # No wren_project.yml anywhere; should return default (~/.wren/project)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WREN_HOME", str(tmp_path / "wren_home"))
    # Re-import to pick up WREN_HOME env var via the module-level constant
    import importlib  # noqa: PLC0415
    import wren.context as ctx  # noqa: PLC0415

    importlib.reload(ctx)
    result = ctx.discover_project_path()
    assert result == tmp_path / "wren_home" / "project"
