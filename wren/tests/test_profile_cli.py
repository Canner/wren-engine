"""Integration tests for the ``wren profile`` CLI sub-app."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

import wren.profile as profile_mod
from wren.profile_cli import profile_app

runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_profiles(tmp_path, monkeypatch):
    """Redirect all profile I/O to a temp directory."""
    profiles_file = tmp_path / "profiles.yml"
    monkeypatch.setattr(profile_mod, "_WREN_HOME", tmp_path)
    monkeypatch.setattr(profile_mod, "_PROFILES_FILE", profiles_file)
    return profiles_file


# ── list ──────────────────────────────────────────────────────────────────────


def test_list_empty():
    result = runner.invoke(profile_app, ["list"])
    assert result.exit_code == 0
    assert "No profiles configured" in result.output


def test_add_then_list():
    runner.invoke(profile_app, ["add", "pg", "--datasource", "postgres"])
    result = runner.invoke(profile_app, ["list"])
    assert result.exit_code == 0
    assert "pg" in result.output
    assert "postgres" in result.output
    assert "*" in result.output  # active marker


def test_list_marks_active_only():
    runner.invoke(profile_app, ["add", "pg", "--datasource", "postgres"])
    runner.invoke(profile_app, ["add", "duck", "--datasource", "duckdb"])
    result = runner.invoke(profile_app, ["list"])
    lines = result.output.splitlines()
    active_lines = [line for line in lines if "*" in line]
    assert len(active_lines) == 1
    assert "pg" in active_lines[0]


# ── add ───────────────────────────────────────────────────────────────────────


def test_add_requires_datasource_or_flag():
    result = runner.invoke(profile_app, ["add", "pg"])
    assert result.exit_code != 0
    assert "--datasource" in result.output or "Error" in result.output


def test_add_from_json_file(tmp_path):
    conn_file = tmp_path / "conn.json"
    conn_file.write_text(
        json.dumps({"datasource": "postgres", "host": "db.local", "port": 5432})
    )
    result = runner.invoke(profile_app, ["add", "pg", "--from-file", str(conn_file)])
    assert result.exit_code == 0
    assert "added" in result.output
    profiles = profile_mod.list_profiles()
    assert profiles["pg"]["host"] == "db.local"


def test_add_from_yaml_file(tmp_path):
    conn_file = tmp_path / "conn.yml"
    conn_file.write_text("datasource: mysql\nhost: mysql.local\nport: 3306\n")
    result = runner.invoke(profile_app, ["add", "my", "--from-file", str(conn_file)])
    assert result.exit_code == 0
    profiles = profile_mod.list_profiles()
    assert profiles["my"]["datasource"] == "mysql"


def test_add_from_file_normalizes_properties_envelope(tmp_path):
    """MCP/web envelope {datasource, properties: {...}} should be flattened."""
    conn_file = tmp_path / "conn.json"
    conn_file.write_text(
        json.dumps(
            {
                "datasource": "duckdb",
                "properties": {"url": "/tmp/warehouse", "format": "duckdb"},
            }
        )
    )
    result = runner.invoke(profile_app, ["add", "duck", "--from-file", str(conn_file)])
    assert result.exit_code == 0
    profiles = profile_mod.list_profiles()
    # After normalization, 'url' should be a top-level key, not nested under 'properties'
    assert "properties" not in profiles["duck"]
    assert profiles["duck"]["url"] == "/tmp/warehouse"
    assert profiles["duck"]["datasource"] == "duckdb"


def test_add_from_file_not_found():
    result = runner.invoke(
        profile_app, ["add", "pg", "--from-file", "/nonexistent/file.json"]
    )
    assert result.exit_code != 0
    assert "not found" in result.output


def test_add_from_file_missing_datasource(tmp_path):
    conn_file = tmp_path / "conn.json"
    conn_file.write_text(json.dumps({"host": "localhost", "port": 5432}))
    result = runner.invoke(profile_app, ["add", "pg", "--from-file", str(conn_file)])
    assert result.exit_code != 0
    assert "datasource" in result.output


def test_add_with_activate_flag():
    runner.invoke(profile_app, ["add", "first", "--datasource", "duckdb"])
    runner.invoke(
        profile_app, ["add", "second", "--datasource", "postgres", "--activate"]
    )
    assert profile_mod.get_active_name() == "second"


def _write_dbt_project(tmp_path):
    project_dir = tmp_path / "jaffle_shop"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text(
        "name: jaffle_shop\n"
        "profile: jaffle_shop\n"
    )
    profiles_path = tmp_path / "dbt_profiles.yml"
    return project_dir, profiles_path


def test_import_dbt_duckdb_profile(tmp_path, monkeypatch):
    project_dir, profiles_path = _write_dbt_project(tmp_path)
    profiles_path.write_text(
        "jaffle_shop:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        "      path: \"{{ env_var('JAFFLE_DUCKDB_PATH') }}\"\n"
    )
    monkeypatch.setenv("JAFFLE_DUCKDB_PATH", "/tmp/jaffle.duckdb")

    result = runner.invoke(
        profile_app,
        [
            "import",
            "dbt",
            "--project-dir",
            str(project_dir),
            "--profiles-path",
            str(profiles_path),
        ],
    )

    assert result.exit_code == 0, result.output
    profiles = profile_mod.list_profiles()
    assert "jaffle-shop-dev" in profiles
    assert profiles["jaffle-shop-dev"]["datasource"] == "duckdb"
    assert profiles["jaffle-shop-dev"]["url"] == "/tmp"
    assert profiles["jaffle-shop-dev"]["format"] == "duckdb"
    assert profile_mod.get_active_name() == "jaffle-shop-dev"


def test_import_dbt_duckdb_relative_path_uses_project_dir(tmp_path):
    project_dir, profiles_path = _write_dbt_project(tmp_path)
    profiles_path.write_text(
        "jaffle_shop:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        "      path: warehouse/jaffle.duckdb\n"
    )

    result = runner.invoke(
        profile_app,
        [
            "import",
            "dbt",
            "--project-dir",
            str(project_dir),
            "--profiles-path",
            str(profiles_path),
        ],
    )

    assert result.exit_code == 0, result.output
    profiles = profile_mod.list_profiles()
    assert profiles["jaffle-shop-dev"]["url"] == str(project_dir / "warehouse")


def test_import_dbt_postgres_profile_custom_name_no_activate(tmp_path):
    project_dir, profiles_path = _write_dbt_project(tmp_path)
    profiles_path.write_text(
        "jaffle_shop:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: postgres\n"
        "      host: localhost\n"
        "      port: 5432\n"
        "      dbname: analytics\n"
        "      user: postgres\n"
        "      password: secret\n"
    )
    runner.invoke(profile_app, ["add", "existing", "--datasource", "duckdb"])

    result = runner.invoke(
        profile_app,
        [
            "import",
            "dbt",
            "--project-dir",
            str(project_dir),
            "--profiles-path",
            str(profiles_path),
            "--name",
            "pg-from-dbt",
            "--no-activate",
        ],
    )

    assert result.exit_code == 0, result.output
    profiles = profile_mod.list_profiles()
    assert profiles["pg-from-dbt"]["datasource"] == "postgres"
    assert profiles["pg-from-dbt"]["database"] == "analytics"
    assert profile_mod.get_active_name() == "existing"


def test_import_dbt_unsupported_source(tmp_path):
    project_dir, profiles_path = _write_dbt_project(tmp_path)
    profiles_path.write_text("{}\n")

    result = runner.invoke(
        profile_app,
        [
            "import",
            "airbyte",
            "--project-dir",
            str(project_dir),
            "--profiles-path",
            str(profiles_path),
        ],
    )

    assert result.exit_code != 0
    assert "Only 'dbt' is supported" in result.output


def test_import_dbt_validation_error(tmp_path):
    project_dir, profiles_path = _write_dbt_project(tmp_path)
    profiles_path.write_text(
        "jaffle_shop:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: postgres\n"
        "      host: localhost\n"
    )

    result = runner.invoke(
        profile_app,
        [
            "import",
            "dbt",
            "--project-dir",
            str(project_dir),
            "--profiles-path",
            str(profiles_path),
        ],
    )

    assert result.exit_code != 0
    assert "missing required field" in result.output


# ── switch ────────────────────────────────────────────────────────────────────


def test_switch_updates_active():
    runner.invoke(profile_app, ["add", "pg", "--datasource", "postgres"])
    runner.invoke(profile_app, ["add", "duck", "--datasource", "duckdb"])
    result = runner.invoke(profile_app, ["switch", "duck"])
    assert result.exit_code == 0
    assert "duck" in result.output
    assert profile_mod.get_active_name() == "duck"

    # list should show * on duck
    list_result = runner.invoke(profile_app, ["list"])
    lines = list_result.output.splitlines()
    duck_line = next(line for line in lines if "duck" in line)
    assert "*" in duck_line


def test_switch_not_found():
    result = runner.invoke(profile_app, ["switch", "ghost"])
    assert result.exit_code != 0
    assert "not found" in result.output


# ── rm ────────────────────────────────────────────────────────────────────────


def test_rm_with_force():
    runner.invoke(profile_app, ["add", "pg", "--datasource", "postgres"])
    result = runner.invoke(profile_app, ["rm", "pg", "--force"])
    assert result.exit_code == 0
    assert "removed" in result.output
    assert "pg" not in profile_mod.list_profiles()


def test_rm_not_found():
    result = runner.invoke(profile_app, ["rm", "ghost", "--force"])
    assert result.exit_code != 0
    assert "not found" in result.output


# ── debug ─────────────────────────────────────────────────────────────────────


def test_debug_output():
    profile_mod.add_profile(
        "pg",
        {
            "datasource": "postgres",
            "host": "db.example.com",
            "password": "topsecret",
        },
    )
    result = runner.invoke(profile_app, ["debug"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["name"] == "pg"
    assert data["config"]["host"] == "db.example.com"
    assert data["config"]["password"] == "***"


def test_debug_no_active_profile():
    result = runner.invoke(profile_app, ["debug"])
    assert result.exit_code != 0
    assert "Error" in result.output


def test_debug_named_profile():
    profile_mod.add_profile("a", {"datasource": "duckdb", "path": ":memory:"})
    profile_mod.add_profile("b", {"datasource": "postgres", "password": "pw"})
    result = runner.invoke(profile_app, ["debug", "b"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["name"] == "b"
    assert data["config"]["password"] == "***"
