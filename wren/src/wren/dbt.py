"""Helpers for reading dbt project configuration and generated artifacts."""

from __future__ import annotations

import base64
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


@dataclass(frozen=True)
class DbtProjectImport:
    """Project files and summary generated from a dbt project."""

    files: list[Any]
    model_count: int
    source_count: int
    skipped_ephemeral: int
    skipped_without_columns: int


def default_wren_profile_name(target: DbtTarget) -> str:
    """Return a stable default Wren profile name for a dbt target."""
    return f"{target.profile_name}-{target.target_name}".replace("_", "-")


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


def convert_dbt_project_to_wren_project(
    project_dir: str | Path,
    *,
    output_dir: str | Path | None = None,
    profiles_path: str | Path | None = None,
    profile_name: str | None = None,
    target_name: str | None = None,
) -> DbtProjectImport:
    """Convert dbt artifacts into Wren project files."""
    from wren.context import (  # noqa: PLC0415
        _AGENTS_MD_TEMPLATE,
        ProjectFile,
    )

    target = resolve_dbt_target(
        project_dir,
        profiles_path=profiles_path,
        profile_name=profile_name,
        target_name=target_name,
    )
    artifacts = load_dbt_artifacts(project_dir, target_path=target.target_path)
    project_root = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else Path.cwd().resolve()
    )

    imported_models, model_count, source_count, skipped_ephemeral, skipped_no_columns = (
        _build_imported_models(artifacts)
    )

    dbt_binding_dir = _relative_or_absolute_path(target.project_dir, project_root)
    project_config = {
        "schema_version": 2,
        "name": artifacts.manifest.get("metadata", {}).get(
            "project_name", target.project.get("name", "dbt_project")
        ),
        "version": str(target.project.get("version", "1.0")),
        "catalog": "wren",
        "schema": "public",
        "data_source": target.datasource,
        "dbt": {
            "project_dir": dbt_binding_dir,
            "profile": target.profile_name,
            "target": target.target_name,
        },
    }

    files = [
        ProjectFile(
            relative_path="wren_project.yml",
            content=yaml.dump(project_config, default_flow_style=False, sort_keys=False),
        ),
        ProjectFile(
            relative_path="relationships.yml",
            content=yaml.dump(
                {"relationships": []},
                default_flow_style=False,
                sort_keys=False,
            ),
        ),
        ProjectFile(
            relative_path="instructions.md",
            content=_build_base_instructions(target, model_count, source_count),
        ),
        ProjectFile(relative_path="AGENTS.md", content=_AGENTS_MD_TEMPLATE),
        ProjectFile(
            relative_path="queries.yml",
            content=yaml.dump(
                {"version": 1, "pairs": []},
                default_flow_style=False,
                sort_keys=False,
            ),
        ),
    ]

    files.extend(
        ProjectFile(
            relative_path=f"models/{model['name']}/metadata.yml",
            content=yaml.dump(model, default_flow_style=False, sort_keys=False),
        )
        for model in imported_models
    )

    return DbtProjectImport(
        files=files,
        model_count=model_count,
        source_count=source_count,
        skipped_ephemeral=skipped_ephemeral,
        skipped_without_columns=skipped_no_columns,
    )


def convert_dbt_target_to_wren_profile(target: DbtTarget) -> dict[str, Any]:
    """Convert a resolved dbt target into a Wren profile payload."""
    output = target.output
    datasource = target.datasource

    if datasource == "duckdb":
        path_value = str(_require_output_field(output, "path"))
        db_path = Path(path_value).expanduser()
        url = db_path if not db_path.suffix else db_path.parent
        if str(url) in {"", "."}:
            url = Path.cwd()
        return {"datasource": "duckdb", "url": str(url), "format": "duckdb"}

    if datasource == "postgres":
        return {
            "datasource": "postgres",
            "host": str(_require_output_field(output, "host")),
            "port": str(output.get("port", "5432")),
            "database": str(_require_output_field(output, "dbname", "database")),
            "user": str(_require_output_field(output, "user")),
            "password": str(output["password"]) if output.get("password") else None,
        }

    if datasource in {"mysql", "redshift", "mssql", "clickhouse"}:
        return _filter_none(
            {
                "datasource": datasource,
                "host": str(_require_output_field(output, "host")),
                "port": str(_require_output_field(output, "port")),
                "database": str(
                    _require_output_field(output, "dbname", "database", "catalog")
                ),
                "user": str(_require_output_field(output, "user")),
                "password": str(output["password"]) if output.get("password") else None,
            }
        )

    if datasource == "snowflake":
        return _filter_none(
            {
                "datasource": "snowflake",
                "account": str(_require_output_field(output, "account")),
                "user": str(_require_output_field(output, "user")),
                "password": str(output["password"]) if output.get("password") else None,
                "database": str(_require_output_field(output, "database")),
                "schema": str(_require_output_field(output, "schema")),
                "warehouse": output.get("warehouse"),
            }
        )

    if datasource == "trino":
        return _filter_none(
            {
                "datasource": "trino",
                "host": str(_require_output_field(output, "host")),
                "port": str(output.get("port", "8080")),
                "catalog": str(_require_output_field(output, "database", "catalog")),
                "schema": str(_require_output_field(output, "schema")),
                "user": output.get("user"),
                "password": (
                    str(output["password"]) if output.get("password") else None
                ),
            }
        )

    if datasource == "athena":
        return _filter_none(
            {
                "datasource": "athena",
                "s3_staging_dir": str(
                    _require_output_field(output, "s3_staging_dir", "s3_data_dir")
                ),
                "region_name": output.get("region_name"),
                "schema_name": output.get("schema", output.get("schema_name")),
                "aws_access_key_id": output.get("aws_access_key_id"),
                "aws_secret_access_key": output.get("aws_secret_access_key"),
                "aws_session_token": output.get("aws_session_token"),
                "role_arn": output.get("role_arn"),
                "role_session_name": output.get("role_session_name"),
            }
        )

    if datasource == "spark":
        return {
            "datasource": "spark",
            "host": str(_require_output_field(output, "host")),
            "port": str(output.get("port", "15002")),
        }

    if datasource == "databricks":
        return {
            "datasource": "databricks",
            "databricks_type": "token",
            "server_hostname": str(
                _require_output_field(output, "server_hostname", "host")
            ),
            "http_path": str(_require_output_field(output, "http_path", "httpPath")),
            "access_token": str(
                _require_output_field(output, "token", "access_token", "accessToken")
            ),
        }

    if datasource == "bigquery":
        credentials = _bigquery_credentials_base64(output)
        return _filter_none(
            {
                "datasource": "bigquery",
                "bigquery_type": "dataset",
                "project_id": str(_require_output_field(output, "project")),
                "dataset_id": str(_require_output_field(output, "dataset")),
                "credentials": credentials,
            }
        )

    raise DbtLoadError(
        f"dbt adapter '{target.adapter_type}' maps to datasource '{datasource}', "
        "but profile conversion has not been implemented yet."
    )


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


def _require_output_field(output: dict[str, Any], *keys: str) -> Any:
    """Return the first present non-empty output field."""
    for key in keys:
        value = output.get(key)
        if value not in (None, ""):
            return value
    names = ", ".join(keys)
    raise DbtLoadError(f"dbt target is missing required field(s): {names}")


def _filter_none(values: dict[str, Any]) -> dict[str, Any]:
    """Drop keys with None values."""
    return {key: value for key, value in values.items() if value is not None}


def _bigquery_credentials_base64(output: dict[str, Any]) -> str:
    """Encode BigQuery credentials from dbt output into Wren's expected format."""
    if output.get("credentials"):
        return str(output["credentials"])

    if output.get("keyfile"):
        keyfile = Path(str(output["keyfile"])).expanduser()
        if not keyfile.exists():
            raise DbtLoadError(f"BigQuery keyfile not found: {keyfile}")
        return base64.b64encode(keyfile.read_bytes()).decode()

    if output.get("keyfile_json"):
        payload = output["keyfile_json"]
        raw = (
            json.dumps(payload, ensure_ascii=False)
            if isinstance(payload, dict)
            else str(payload)
        )
        return base64.b64encode(raw.encode("utf-8")).decode()

    raise DbtLoadError(
        "BigQuery dbt target requires one of: credentials, keyfile, or keyfile_json."
    )


def _build_imported_models(
    artifacts: DbtArtifacts,
) -> tuple[list[dict[str, Any]], int, int, int, int]:
    manifest = artifacts.manifest
    catalog_nodes = artifacts.catalog.get("nodes", {})
    catalog_sources = artifacts.catalog.get("sources", {})

    imported_models: list[dict[str, Any]] = []
    used_names: set[str] = set()
    model_count = 0
    source_count = 0
    skipped_ephemeral = 0
    skipped_without_columns = 0

    for unique_id, node in sorted(manifest.get("nodes", {}).items()):
        if node.get("resource_type") != "model":
            continue
        if str(node.get("config", {}).get("materialized", "")).lower() == "ephemeral":
            skipped_ephemeral += 1
            continue
        model = _build_model_metadata(
            unique_id=unique_id,
            node=node,
            catalog_entry=catalog_nodes.get(unique_id, {}),
            wren_name=str(node.get("alias") or node.get("name") or ""),
            dbt_resource_type="model",
        )
        if model is None:
            skipped_without_columns += 1
            continue
        _ensure_unique_model_name(model["name"], used_names)
        imported_models.append(model)
        model_count += 1

    for unique_id, node in sorted(manifest.get("sources", {}).items()):
        source_name = _choose_source_model_name(node, used_names)
        model = _build_model_metadata(
            unique_id=unique_id,
            node=node,
            catalog_entry=catalog_sources.get(unique_id, {}),
            wren_name=source_name,
            dbt_resource_type="source",
        )
        if model is None:
            skipped_without_columns += 1
            continue
        _ensure_unique_model_name(model["name"], used_names)
        imported_models.append(model)
        source_count += 1

    return (
        imported_models,
        model_count,
        source_count,
        skipped_ephemeral,
        skipped_without_columns,
    )


def _build_model_metadata(
    *,
    unique_id: str,
    node: dict[str, Any],
    catalog_entry: dict[str, Any],
    wren_name: str,
    dbt_resource_type: str,
) -> dict[str, Any] | None:
    columns = _extract_columns(node, catalog_entry)
    if not columns:
        return None

    table_name = (
        node.get("identifier")
        or node.get("alias")
        or node.get("relation_name")
        or node.get("name")
    )
    if not table_name:
        raise DbtLoadError(f"dbt node '{unique_id}' is missing a relation name.")

    properties = _filter_none(
        {
            "description": _clean_description(node.get("description")),
            "dbt_layer": infer_dbt_layer(node),
            "dbt_unique_id": unique_id,
            "dbt_resource_type": dbt_resource_type,
        }
    )

    return {
        "name": wren_name,
        "table_reference": {
            "catalog": node.get("database", ""),
            "schema": node.get("schema", ""),
            "table": str(table_name),
        },
        "columns": columns,
        "cached": False,
        "properties": properties,
    }


def _extract_columns(node: dict[str, Any], catalog_entry: dict[str, Any]) -> list[dict]:
    manifest_columns = node.get("columns", {}) or {}
    catalog_columns = catalog_entry.get("columns", {}) or {}

    merged_names = list(manifest_columns)
    for name in catalog_columns:
        if name not in merged_names:
            merged_names.append(name)

    def _sort_key(name: str) -> tuple[int, int, str]:
        catalog_index = catalog_columns.get(name, {}).get("index")
        return (0 if catalog_index is not None else 1, catalog_index or 0, name)

    columns: list[dict] = []
    for name in sorted(merged_names, key=_sort_key):
        manifest_col = manifest_columns.get(name, {}) or {}
        catalog_col = catalog_columns.get(name, {}) or {}
        description = _clean_description(manifest_col.get("description"))
        data_type = catalog_col.get("type") or manifest_col.get("data_type") or "VARCHAR"
        column = {
            "name": name,
            "type": str(data_type).upper(),
            "is_calculated": False,
            "not_null": False,
            "properties": _filter_none({"description": description}),
        }
        columns.append(column)
    return columns


def infer_dbt_layer(node: dict[str, Any]) -> str:
    """Infer a dbt layer from resource metadata."""
    if node.get("resource_type") == "source":
        return "raw"

    fqn = [str(part).lower() for part in node.get("fqn", [])]
    name = str(node.get("name") or "").lower()
    materialized = str(node.get("config", {}).get("materialized") or "").lower()

    if materialized == "ephemeral":
        return "ephemeral"
    if any("staging" == part for part in fqn) or name.startswith("stg_"):
        return "staging"
    if any("marts" == part for part in fqn) or name.startswith(("fct_", "dim_")):
        return "mart"
    if any("intermediate" == part for part in fqn) or name.startswith("int_"):
        return "intermediate"
    return "model"


def _choose_source_model_name(node: dict[str, Any], used_names: set[str]) -> str:
    base = f"raw_{node.get('name')}"
    if base not in used_names:
        return base
    source_name = node.get("source_name") or "source"
    return f"raw_{source_name}_{node.get('name')}"


def _ensure_unique_model_name(name: str, used_names: set[str]) -> None:
    if name in used_names:
        raise DbtLoadError(
            f"Duplicate Wren model name '{name}' generated from dbt artifacts. "
            "Use dbt aliases or adjust the import naming strategy."
        )
    used_names.add(name)


def _clean_description(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _build_base_instructions(
    target: DbtTarget,
    model_count: int,
    source_count: int,
) -> str:
    return (
        "# Imported from dbt\n\n"
        f"- dbt project: `{target.project.get('name', target.project_dir.name)}`\n"
        f"- dbt profile/target: `{target.profile_name}.{target.target_name}`\n"
        f"- imported models: {model_count}\n"
        f"- imported sources: {source_count}\n\n"
        "This is the base dbt import. Structural metadata comes from "
        "`manifest.json` and `catalog.json`. Additional enrichment from dbt "
        "tests and compiled SQL can be layered on in later steps.\n"
    )


def _relative_or_absolute_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return os.path.relpath(path, root)
