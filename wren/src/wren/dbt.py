"""Helpers for reading dbt project configuration and generated artifacts."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_DBT_TARGET_PATH = "target"
_COMPILED_DIR = "compiled"
_DBT_PROJECT_FILE = "dbt_project.yml"
_DBT_PROFILES_FILE = "profiles.yml"
_MANIFEST_FILE = "manifest.json"
_CATALOG_FILE = "catalog.json"
_RUN_RESULTS_FILE = "run_results.json"

_ENV_VAR_PATTERN = re.compile(
    r"""
    \{\{\s*
    env_var
    \(\s*
    (?P<quote1>['"])
    (?P<name>[^'"]+)
    (?P=quote1)
    (?:\s*,\s*
        (?P<quote2>['"])
        (?P<default>[^'"]*)
        (?P=quote2)
    )?
    \s*\)
    \s*\}\}
    """,
    re.VERBOSE,
)

DBT_ADAPTER_TO_WREN_DATASOURCE = {
    "athena": "athena",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "doris": "mysql",
    "duckdb": "duckdb",
    "mysql": "mysql",
    "postgres": "postgres",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "spark": "spark",
    "sqlserver": "mssql",
    "trino": "trino",
}


class DbtLoadError(ValueError):
    """Raised when dbt configuration or artifacts cannot be loaded."""


@dataclass(frozen=True)
class DbtTarget:
    """Resolved dbt target configuration."""

    project_dir: Path
    profile_name: str
    target_name: str
    target_path: Path
    adapter_type: str
    datasource: str
    project: dict[str, Any]
    profile: dict[str, Any]
    output: dict[str, Any]


@dataclass(frozen=True)
class DbtArtifacts:
    """Loaded dbt artifacts for a project/target."""

    project_dir: Path
    target_path: Path
    manifest: dict[str, Any]
    catalog: dict[str, Any]
    run_results: dict[str, Any] | None
    compiled_sql: dict[str, str]


def map_dbt_adapter_to_wren(adapter_type: str) -> str:
    """Map a dbt adapter name to a Wren datasource name."""
    normalized = adapter_type.strip().lower()
    try:
        return DBT_ADAPTER_TO_WREN_DATASOURCE[normalized]
    except KeyError as exc:
        raise DbtLoadError(
            f"Unsupported dbt adapter '{adapter_type}'. "
            "Add a datasource mapping before importing this profile."
        ) from exc


def resolve_env_vars(value: Any, env: dict[str, str] | None = None) -> Any:
    """Recursively resolve dbt ``env_var()`` references inside YAML values."""
    env_map = env if env is not None else os.environ

    if isinstance(value, dict):
        return {k: resolve_env_vars(v, env=env_map) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_env_vars(v, env=env_map) for v in value]
    if not isinstance(value, str):
        return value

    def _replace(match: re.Match[str]) -> str:
        name = match.group("name")
        default = match.group("default")
        if name in env_map:
            return env_map[name]
        if default is not None:
            return default
        raise DbtLoadError(
            f"Environment variable '{name}' is required by dbt config but is not set."
        )

    return _ENV_VAR_PATTERN.sub(_replace, value)


def load_dbt_project(project_dir: str | Path) -> dict[str, Any]:
    """Load ``dbt_project.yml`` from a dbt project directory."""
    root = Path(project_dir).expanduser().resolve()
    project_file = root / _DBT_PROJECT_FILE
    if not project_file.exists():
        raise DbtLoadError(
            f"dbt project file not found: {project_file}. "
            "Expected a directory containing dbt_project.yml."
        )
    project = _load_yaml_file(project_file, label="dbt project")
    if not isinstance(project, dict):
        raise DbtLoadError(
            f"dbt project file must contain a YAML mapping: {project_file}"
        )
    return project


def load_dbt_profiles(
    profiles_path: str | Path | None = None,
) -> dict[str, dict[str, Any]]:
    """Load dbt ``profiles.yml`` as a mapping of profile name to config."""
    path = (
        Path(profiles_path).expanduser()
        if profiles_path is not None
        else Path.home() / ".dbt" / _DBT_PROFILES_FILE
    )
    profiles = _load_yaml_file(path, label="dbt profiles")
    if profiles is None:
        raise DbtLoadError(f"dbt profiles file is empty: {path}")
    if not isinstance(profiles, dict):
        raise DbtLoadError(
            f"dbt profiles file must contain a YAML mapping: {path}"
        )
    return profiles


def resolve_dbt_target(
    project_dir: str | Path,
    *,
    profiles_path: str | Path | None = None,
    profile_name: str | None = None,
    target_name: str | None = None,
    env: dict[str, str] | None = None,
) -> DbtTarget:
    """Resolve the active dbt profile and target for a project."""
    root = Path(project_dir).expanduser().resolve()
    project = load_dbt_project(root)
    selected_profile_name = profile_name or project.get("profile")
    if not selected_profile_name:
        raise DbtLoadError(
            "dbt project is missing 'profile'; pass --profile explicitly."
        )

    profiles = load_dbt_profiles(profiles_path)
    if selected_profile_name not in profiles:
        available = ", ".join(sorted(profiles)) or "none"
        raise DbtLoadError(
            f"dbt profile '{selected_profile_name}' not found in profiles.yml. "
            f"Available profiles: {available}."
        )

    profile = profiles[selected_profile_name]
    if not isinstance(profile, dict):
        raise DbtLoadError(
            f"dbt profile '{selected_profile_name}' must be a mapping."
        )

    outputs = profile.get("outputs")
    if not isinstance(outputs, dict) or not outputs:
        raise DbtLoadError(
            f"dbt profile '{selected_profile_name}' is missing 'outputs'."
        )

    selected_target_name = target_name or profile.get("target")
    if not selected_target_name:
        raise DbtLoadError(
            f"dbt profile '{selected_profile_name}' is missing 'target'. "
            "Pass --target explicitly."
        )
    if selected_target_name not in outputs:
        available_targets = ", ".join(sorted(outputs)) or "none"
        raise DbtLoadError(
            f"dbt target '{selected_target_name}' not found in profile "
            f"'{selected_profile_name}'. Available targets: {available_targets}."
        )

    resolved_output = resolve_env_vars(outputs[selected_target_name], env=env)
    if not isinstance(resolved_output, dict):
        raise DbtLoadError(
            f"dbt target '{selected_target_name}' must resolve to a mapping."
        )
    adapter_type = str(resolved_output.get("type") or "").strip()
    if not adapter_type:
        raise DbtLoadError(
            f"dbt target '{selected_target_name}' is missing adapter 'type'."
        )

    target_dir_name = str(project.get("target-path") or _DEFAULT_DBT_TARGET_PATH)
    target_path = root / target_dir_name

    return DbtTarget(
        project_dir=root,
        profile_name=selected_profile_name,
        target_name=selected_target_name,
        target_path=target_path,
        adapter_type=adapter_type,
        datasource=map_dbt_adapter_to_wren(adapter_type),
        project=project,
        profile=profile,
        output=resolved_output,
    )


def load_dbt_artifacts(
    project_dir: str | Path,
    *,
    target_path: str | Path | None = None,
) -> DbtArtifacts:
    """Load the dbt artifacts needed for Wren import."""
    root = Path(project_dir).expanduser().resolve()
    project = load_dbt_project(root)
    resolved_target_path = (
        Path(target_path).expanduser().resolve()
        if target_path is not None
        else root / str(project.get("target-path") or _DEFAULT_DBT_TARGET_PATH)
    )

    manifest = _load_json_file(
        resolved_target_path / _MANIFEST_FILE,
        label="dbt manifest",
    )
    catalog = _load_json_file(
        resolved_target_path / _CATALOG_FILE,
        label="dbt catalog",
    )

    run_results_path = resolved_target_path / _RUN_RESULTS_FILE
    run_results = (
        _load_json_file(run_results_path, label="dbt run results")
        if run_results_path.exists()
        else None
    )

    compiled_sql = load_compiled_sql(resolved_target_path / _COMPILED_DIR)

    return DbtArtifacts(
        project_dir=root,
        target_path=resolved_target_path,
        manifest=manifest,
        catalog=catalog,
        run_results=run_results,
        compiled_sql=compiled_sql,
    )


def load_compiled_sql(compiled_dir: str | Path) -> dict[str, str]:
    """Load compiled SQL files keyed by their relative path."""
    root = Path(compiled_dir).expanduser()
    if not root.exists():
        return {}

    sql_files = sorted(path for path in root.rglob("*.sql") if path.is_file())
    return {
        str(path.relative_to(root)): path.read_text(encoding="utf-8")
        for path in sql_files
    }


def _load_yaml_file(path: Path, *, label: str) -> Any:
    """Load YAML from *path* with a consistent error surface."""
    if not path.exists():
        raise DbtLoadError(f"{label} file not found: {path}")
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise DbtLoadError(f"{label} is not valid YAML: {path}: {exc}") from exc


def _load_json_file(path: Path, *, label: str) -> dict[str, Any]:
    """Load JSON from *path* with a consistent error surface."""
    if not path.exists():
        raise DbtLoadError(f"{label} file not found: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DbtLoadError(f"{label} is not valid JSON: {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise DbtLoadError(f"{label} must contain a JSON object: {path}")
    return data
