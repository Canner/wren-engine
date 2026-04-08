"""Unit tests for wren.dlt_introspect and context.convert_dlt_to_project."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from wren.cli import app
from wren.context import convert_dlt_to_project
from wren.dlt_introspect import DltIntrospector

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

runner = CliRunner()


@pytest.fixture()
def dlt_duckdb(tmp_path: Path):
    """Create a DuckDB file that mimics a dlt pipeline output."""
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / "test_pipeline.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("""
        CREATE TABLE hubspot__contacts (
            _dlt_id      VARCHAR,
            _dlt_load_id VARCHAR,
            id           BIGINT,
            email        VARCHAR,
            first_name   VARCHAR,
            created_at   TIMESTAMP
        );
        CREATE TABLE hubspot__contacts__emails (
            _dlt_id      VARCHAR,
            _dlt_parent_id VARCHAR,
            _dlt_load_id VARCHAR,
            _dlt_list_idx BIGINT,
            email_address VARCHAR,
            email_type    VARCHAR
        );
        CREATE TABLE _dlt_loads (
            load_id VARCHAR,
            status  VARCHAR
        );
        CREATE TABLE _dlt_pipeline_state (
            version BIGINT,
            state   VARCHAR
        );
    """)
    con.close()
    return db_path


@pytest.fixture()
def empty_duckdb(tmp_path: Path):
    """DuckDB file with no user tables."""
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / "empty.duckdb"
    duckdb.connect(str(db_path)).close()
    return db_path


@pytest.fixture()
def multi_schema_duckdb(tmp_path: Path):
    """DuckDB file with tables in multiple schemas."""
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / "multi.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("""
        CREATE SCHEMA crm;
        CREATE TABLE crm.contacts (id BIGINT, name VARCHAR);
        CREATE TABLE orders (order_id BIGINT, amount DOUBLE);
    """)
    con.close()
    return db_path


@pytest.fixture()
def orphan_child_duckdb(tmp_path: Path):
    """Child table with _dlt_parent_id but no matching parent."""
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / "orphan.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("""
        CREATE TABLE orphan__child (
            _dlt_id        VARCHAR,
            _dlt_parent_id VARCHAR,
            value          INTEGER
        );
    """)
    con.close()
    return db_path


# ---------------------------------------------------------------------------
# DltIntrospector tests
# ---------------------------------------------------------------------------


class TestDiscoverTables:
    def test_finds_user_tables(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            tables, _ = intro.introspect()

        names = {t.name for t in tables}
        assert "hubspot__contacts" in names
        assert "hubspot__contacts__emails" in names

    def test_excludes_dlt_internal_tables(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            tables, _ = intro.introspect()

        names = {t.name for t in tables}
        assert "_dlt_loads" not in names
        assert "_dlt_pipeline_state" not in names

    def test_empty_database_returns_no_tables(self, empty_duckdb):
        with DltIntrospector(empty_duckdb) as intro:
            tables, rels = intro.introspect()

        assert tables == []
        assert rels == []

    def test_multiple_schemas_discovered(self, multi_schema_duckdb):
        with DltIntrospector(multi_schema_duckdb) as intro:
            tables, _ = intro.introspect()

        names = {t.name for t in tables}
        assert "contacts" in names
        assert "orders" in names

    def test_schema_field_populated(self, multi_schema_duckdb):
        with DltIntrospector(multi_schema_duckdb) as intro:
            tables, _ = intro.introspect()

        schema_map = {t.name: t.schema for t in tables}
        assert schema_map["contacts"] == "crm"
        assert schema_map["orders"] == "main"


class TestFilterDltColumns:
    def test_excludes_dlt_columns_from_contacts(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            tables, _ = intro.introspect()

        contacts = next(t for t in tables if t.name == "hubspot__contacts")
        col_names = {c.name for c in contacts.columns}
        assert "_dlt_id" not in col_names
        assert "_dlt_load_id" not in col_names
        # User columns preserved
        assert "id" in col_names
        assert "email" in col_names

    def test_excludes_dlt_columns_from_child(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            tables, _ = intro.introspect()

        emails = next(t for t in tables if t.name == "hubspot__contacts__emails")
        col_names = {c.name for c in emails.columns}
        assert "_dlt_parent_id" not in col_names
        assert "_dlt_list_idx" not in col_names
        assert "email_address" in col_names

    def test_has_dlt_parent_id_flag(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            tables, _ = intro.introspect()

        contacts = next(t for t in tables if t.name == "hubspot__contacts")
        emails = next(t for t in tables if t.name == "hubspot__contacts__emails")
        assert contacts.has_dlt_parent_id is False
        assert emails.has_dlt_parent_id is True


class TestTypeNormalization:
    def test_bigint_normalized(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            tables, _ = intro.introspect()

        contacts = next(t for t in tables if t.name == "hubspot__contacts")
        col = next(c for c in contacts.columns if c.name == "id")
        assert col.normalized_type == "BIGINT"

    def test_varchar_normalized(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            tables, _ = intro.introspect()

        contacts = next(t for t in tables if t.name == "hubspot__contacts")
        col = next(c for c in contacts.columns if c.name == "email")
        # sqlglot normalizes VARCHAR → TEXT in the duckdb dialect
        assert col.normalized_type in ("VARCHAR", "TEXT")

    def test_timestamp_normalized(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            tables, _ = intro.introspect()

        contacts = next(t for t in tables if t.name == "hubspot__contacts")
        col = next(c for c in contacts.columns if c.name == "created_at")
        # sqlglot may produce TIMESTAMP or TIMESTAMP WITH TIME ZONE — just check it's set
        assert col.normalized_type


class TestDetectRelationships:
    def test_detects_parent_child(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            _, rels = intro.introspect()

        assert len(rels) == 1
        rel = rels[0]
        assert rel["models"] == ["hubspot__contacts", "hubspot__contacts__emails"]
        assert rel["join_type"] == "ONE_TO_MANY"
        assert "_dlt_parent_id" in rel["condition"]
        assert "_dlt_id" in rel["condition"]

    def test_relationship_name(self, dlt_duckdb):
        with DltIntrospector(dlt_duckdb) as intro:
            _, rels = intro.introspect()

        assert rels[0]["name"] == "hubspot__contacts_emails"

    def test_no_parent_found_emits_warning(self, orphan_child_duckdb, caplog):
        with caplog.at_level(logging.WARNING, logger="wren.dlt_introspect"):
            with DltIntrospector(orphan_child_duckdb) as intro:
                _, rels = intro.introspect()

        assert rels == []
        assert any("no matching parent" in r.message for r in caplog.records)

    def test_no_parent_found_skips_relationship(self, orphan_child_duckdb):
        with DltIntrospector(orphan_child_duckdb) as intro:
            _, rels = intro.introspect()

        assert rels == []


# ---------------------------------------------------------------------------
# convert_dlt_to_project tests
# ---------------------------------------------------------------------------


class TestConvertDltToProject:
    def test_produces_project_file(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb)
        paths = {f.relative_path for f in files}
        assert "wren_project.yml" in paths

    def test_wren_project_yml_content(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb)
        proj = next(f for f in files if f.relative_path == "wren_project.yml")
        data = yaml.safe_load(proj.content)
        assert data["schema_version"] == 2
        assert data["data_source"] == "duckdb"
        assert data["name"] == "test_pipeline"

    def test_custom_project_name(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb, project_name="my_project")
        proj = next(f for f in files if f.relative_path == "wren_project.yml")
        data = yaml.safe_load(proj.content)
        assert data["name"] == "my_project"

    def test_model_files_generated(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb)
        paths = {f.relative_path for f in files}
        assert "models/hubspot__contacts/metadata.yml" in paths
        assert "models/hubspot__contacts__emails/metadata.yml" in paths

    def test_model_content(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb)
        meta = next(
            f
            for f in files
            if f.relative_path == "models/hubspot__contacts/metadata.yml"
        )
        data = yaml.safe_load(meta.content)
        assert data["name"] == "hubspot__contacts"
        assert data["table_reference"]["table"] == "hubspot__contacts"
        assert data["table_reference"]["schema"] == "main"
        col_names = [c["name"] for c in data["columns"]]
        assert "id" in col_names
        assert "email" in col_names
        # dlt columns must be excluded
        assert "_dlt_id" not in col_names
        assert "_dlt_load_id" not in col_names

    def test_relationships_file_generated(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb)
        paths = {f.relative_path for f in files}
        assert "relationships.yml" in paths

    def test_relationships_content(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb)
        rel_file = next(f for f in files if f.relative_path == "relationships.yml")
        data = yaml.safe_load(rel_file.content)
        rels = data["relationships"]
        assert len(rels) == 1
        assert rels[0]["join_type"] == "ONE_TO_MANY"

    def test_instructions_file_generated(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb)
        paths = {f.relative_path for f in files}
        assert "instructions.md" in paths

    def test_instructions_content(self, dlt_duckdb):
        files = convert_dlt_to_project(dlt_duckdb)
        inst = next(f for f in files if f.relative_path == "instructions.md")
        assert "dlt" in inst.content
        assert "test_pipeline" in inst.content

    def test_empty_database_no_models(self, empty_duckdb):
        files = convert_dlt_to_project(empty_duckdb)
        model_files = [f for f in files if f.relative_path.startswith("models/")]
        assert model_files == []

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            convert_dlt_to_project(tmp_path / "nonexistent.duckdb")


# ---------------------------------------------------------------------------
# CLI integration test
# ---------------------------------------------------------------------------


class TestInitFromDlt:
    def test_init_from_dlt_creates_project(self, dlt_duckdb, tmp_path):
        result = runner.invoke(
            app,
            ["context", "init", "--from-dlt", str(dlt_duckdb), "--path", str(tmp_path)],
        )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "wren_project.yml").exists()
        assert (tmp_path / "models" / "hubspot__contacts" / "metadata.yml").exists()
        assert (
            tmp_path / "models" / "hubspot__contacts__emails" / "metadata.yml"
        ).exists()
        assert (tmp_path / "relationships.yml").exists()

    def test_init_from_dlt_summary_output(self, dlt_duckdb, tmp_path):
        result = runner.invoke(
            app,
            ["context", "init", "--from-dlt", str(dlt_duckdb), "--path", str(tmp_path)],
        )
        assert "2 models" in result.output
        assert "1 relationships" in result.output

    def test_init_from_dlt_missing_file(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "context",
                "init",
                "--from-dlt",
                str(tmp_path / "missing.duckdb"),
                "--path",
                str(tmp_path),
            ],
        )
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_init_from_dlt_and_mdl_mutually_exclusive(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "context",
                "init",
                "--from-dlt",
                "some.duckdb",
                "--from-mdl",
                "some.json",
                "--path",
                str(tmp_path),
            ],
        )
        assert result.exit_code == 1
        assert "cannot be used together" in result.output

    def test_init_from_dlt_refuses_existing_without_force(self, dlt_duckdb, tmp_path):
        (tmp_path / "wren_project.yml").write_text("name: existing\n")
        result = runner.invoke(
            app,
            ["context", "init", "--from-dlt", str(dlt_duckdb), "--path", str(tmp_path)],
        )
        assert result.exit_code == 1
        assert "already exists" in result.output

    def test_init_from_dlt_force_overwrites(self, dlt_duckdb, tmp_path):
        (tmp_path / "wren_project.yml").write_text("name: existing\n")
        result = runner.invoke(
            app,
            [
                "context",
                "init",
                "--from-dlt",
                str(dlt_duckdb),
                "--path",
                str(tmp_path),
                "--force",
            ],
        )
        assert result.exit_code == 0, result.output
