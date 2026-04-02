"""Context management — load YAML MDL files, validate, build manifest JSON."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import yaml

_WREN_HOME = Path(os.environ.get("WREN_HOME", Path.home() / ".wren"))
_DEFAULT_PROJECT = _WREN_HOME / "project"
_PROJECT_FILE = "wren_project.yml"
_TARGET_DIR = "target"
_TARGET_FILE = "mdl.json"


# ── Case conversion ───────────────────────────────────────────────────────


def _snake_to_camel(name: str) -> str:
    """Convert snake_case to camelCase."""
    parts = name.split("_")
    return parts[0] + "".join(w.capitalize() for w in parts[1:])


def _convert_keys(obj: Any) -> Any:
    """Recursively convert all dict keys from snake_case to camelCase."""
    if isinstance(obj, dict):
        return {_snake_to_camel(k): _convert_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_keys(item) for item in obj]
    return obj


# ── Project discovery ─────────────────────────────────────────────────────


def discover_project_path(explicit: str | None = None) -> Path:
    """Return the project directory path.

    Priority: explicit arg > walk up from cwd to find wren_project.yml > ~/.wren/project/.
    """
    if explicit:
        return Path(explicit).expanduser()

    # Walk up from cwd looking for wren_project.yml
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / _PROJECT_FILE).exists():
            return parent
        # Stop at home or root
        if parent == Path.home() or parent == parent.parent:
            break

    return _DEFAULT_PROJECT


def load_project_config(project_path: Path) -> dict:
    """Load wren_project.yml and return as dict."""
    config_file = project_path / _PROJECT_FILE
    if not config_file.exists():
        return {}
    return yaml.safe_load(config_file.read_text()) or {}


_SUPPORTED_SCHEMA_VERSIONS = {1, 2}


def get_schema_version(project_path: Path) -> int:
    """Return the schema_version from wren_project.yml (default 1)."""
    config = load_project_config(project_path)
    raw = config.get("schema_version", 1)
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise SystemExit(
            f"Error: invalid schema_version {raw!r} in {_PROJECT_FILE}. Expected an integer."
        )


def require_schema_version(project_path: Path) -> int:
    """Return schema_version or raise SystemExit if unsupported."""
    sv = get_schema_version(project_path)
    if sv not in _SUPPORTED_SCHEMA_VERSIONS:
        raise SystemExit(
            f"Error: unsupported schema_version {sv} in {_PROJECT_FILE}. "
            "Please upgrade wren CLI."
        )
    return sv


# ── Loaders (all return snake_case dicts) ─────────────────────────────────


def load_models(project_path: Path) -> list[dict]:
    """Load models — dispatches on schema_version.

    v1 (legacy): models/*.yml (flat files)
    v2: models/<name>/metadata.yml + optional ref_sql.sql
    """
    sv = get_schema_version(project_path)
    if sv == 1:
        return _load_models_v1(project_path)
    return _load_models_v2(project_path)


def _load_models_v1(project_path: Path) -> list[dict]:
    """Legacy: load model YAML files from project_path/models/*.yml."""
    models_dir = project_path / "models"
    if not models_dir.is_dir():
        return []
    models = []
    for f in sorted(models_dir.glob("*.yml")):
        data = yaml.safe_load(f.read_text())
        if isinstance(data, dict):
            data["_source_dir"] = f.stem
            models.append(data)
    return models


def _load_models_v2(project_path: Path) -> list[dict]:
    """v2: load models from project_path/models/<model_name>/ directories.

    Each model directory must contain metadata.yml and optionally ref_sql.sql.
    If ref_sql.sql exists, its content is used as the model's `ref_sql` field
    and takes precedence over any `ref_sql` defined inline in metadata.yml.
    """
    models_dir = project_path / "models"
    if not models_dir.is_dir():
        return []
    models = []
    for d in sorted(models_dir.iterdir()):
        if not d.is_dir():
            continue
        meta_file = d / "metadata.yml"
        if not meta_file.exists():
            continue
        model = yaml.safe_load(meta_file.read_text()) or {}
        if not isinstance(model, dict):
            continue
        model["_source_dir"] = d.name

        # Merge ref_sql.sql if present (takes precedence)
        ref_sql_file = d / "ref_sql.sql"
        if ref_sql_file.exists():
            sql_content = ref_sql_file.read_text().strip()
            if sql_content:
                model["ref_sql"] = sql_content

        models.append(model)
    return models


def load_views(project_path: Path) -> list[dict]:
    """Load views — dispatches on schema_version.

    v1 (legacy): views.yml (single file with `views:` list)
    v2: views/<name>/metadata.yml + optional sql.yml
    """
    sv = get_schema_version(project_path)
    if sv == 1:
        return _load_views_v1(project_path)
    return _load_views_v2(project_path)


def _load_views_v1(project_path: Path) -> list[dict]:
    """Legacy: load views from project_path/views.yml."""
    views_file = project_path / "views.yml"
    if not views_file.exists():
        return []
    data = yaml.safe_load(views_file.read_text()) or {}
    return data.get("views", []) if isinstance(data, dict) else []


def _load_views_v2(project_path: Path) -> list[dict]:
    """v2: load views from project_path/views/<view_name>/ directories.

    Each view directory must contain metadata.yml and optionally sql.yml.
    If sql.yml exists, its `statement` field takes precedence over any
    `statement` defined inline in metadata.yml.
    """
    views_dir = project_path / "views"
    if not views_dir.is_dir():
        return []
    views = []
    for d in sorted(views_dir.iterdir()):
        if not d.is_dir():
            continue
        meta_file = d / "metadata.yml"
        if not meta_file.exists():
            continue
        view = yaml.safe_load(meta_file.read_text()) or {}
        if not isinstance(view, dict):
            continue
        view["_source_dir"] = d.name

        # Merge sql.yml if present (takes precedence)
        sql_file = d / "sql.yml"
        if sql_file.exists():
            sql_data = yaml.safe_load(sql_file.read_text()) or {}
            if isinstance(sql_data, dict) and sql_data.get("statement"):
                view["statement"] = sql_data["statement"]

        views.append(view)
    return views


def load_relationships(project_path: Path) -> list[dict]:
    """Load relationships from project_path/relationships.yml."""
    rel_file = project_path / "relationships.yml"
    if not rel_file.exists():
        return []
    data = yaml.safe_load(rel_file.read_text()) or {}
    return data.get("relationships", []) if isinstance(data, dict) else []


def load_instructions(project_path: Path) -> str | None:
    """Load instructions.md as a string."""
    inst_file = project_path / "instructions.md"
    if not inst_file.exists():
        return None
    return inst_file.read_text().strip() or None


# ── Build ─────────────────────────────────────────────────────────────────


def build_manifest(project_path: Path) -> dict:
    """Build a complete MDL manifest dict from the project directory.

    Returns the manifest in snake_case (YAML-native form).
    Use _convert_keys() to get the camelCase JSON form.
    """
    project_config = load_project_config(project_path)
    models = load_models(project_path)
    views = load_views(project_path)
    relationships = load_relationships(project_path)
    instructions = load_instructions(project_path)

    # Strip internal metadata
    for m in models:
        m.pop("_source_dir", None)
    for v in views:
        v.pop("_source_dir", None)

    manifest: dict[str, Any] = {
        "catalog": project_config.get("catalog", "wren"),
        "schema": project_config.get("schema", "public"),
        "models": models,
        "relationships": relationships,
        "views": views,
    }

    if instructions:
        manifest["_instructions"] = instructions

    return manifest


def build_json(project_path: Path) -> dict:
    """Build the final camelCase JSON manifest for the engine."""
    manifest = build_manifest(project_path)
    # _instructions is a special key — keep as-is (not a YAML schema field)
    instructions = manifest.pop("_instructions", None)
    result = _convert_keys(manifest)
    if instructions:
        result["_instructions"] = instructions
    return result


def save_target(manifest_json: dict, project_path: Path) -> Path:
    """Write mdl.json to target/ directory. Returns the output path."""
    target_dir = project_path / _TARGET_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    out = target_dir / _TARGET_FILE
    out.write_text(json.dumps(manifest_json, indent=2, ensure_ascii=False))
    return out


# ── Validation ────────────────────────────────────────────────────────────


class ValidationError:
    """A single validation issue."""

    def __init__(self, level: str, path: str, message: str):
        self.level = level  # "error" | "warning"
        self.path = path  # e.g. "models/orders.yml > column 'foo'"
        self.message = message

    def __str__(self):
        return f"[{self.level.upper()}] {self.path}: {self.message}"


def validate_project(project_path: Path) -> list[ValidationError]:
    """Validate project-level structure.

    Checks:
    0. wren_project.yml exists and has required fields
    1. Every model has a name and at least one column
    2. Every column has name and type
    3. Model has exactly one of table_reference or ref_sql (not both, not neither)
    4. Relationship references existing model names
    5. No duplicate model/view names
    6. Views have a statement
    7. primary_key column exists in the model's columns
    8. table_reference (if used) has at least a table field
    """
    errors: list[ValidationError] = []

    # Check project config
    config = load_project_config(project_path)
    if not config:
        errors.append(
            ValidationError(
                "error", _PROJECT_FILE, f"'{_PROJECT_FILE}' not found or empty"
            )
        )
    else:
        for required in ("name", "data_source"):
            if not config.get(required):
                errors.append(
                    ValidationError(
                        "error", _PROJECT_FILE, f"missing required field '{required}'"
                    )
                )
        raw_sv = config.get("schema_version", 1)
        try:
            sv = int(raw_sv)
        except (TypeError, ValueError):
            errors.append(
                ValidationError(
                    "error",
                    _PROJECT_FILE,
                    f"schema_version must be an integer, got {raw_sv!r}",
                )
            )
            sv = 1
        if sv not in _SUPPORTED_SCHEMA_VERSIONS:
            errors.append(
                ValidationError(
                    "error",
                    _PROJECT_FILE,
                    f"unsupported schema_version {sv} — please upgrade wren CLI",
                )
            )

    # Load data (snake_case)
    models = load_models(project_path)
    views = load_views(project_path)
    relationships = load_relationships(project_path)

    model_names: set[str] = set()
    view_names: set[str] = set()

    # Check models
    for i, model in enumerate(models):
        src = model.get("_source_dir", f"models[{i}]")
        src_path = f"models/{src}/metadata.yml"
        name = model.get("name")
        if not name:
            errors.append(ValidationError("error", src_path, "model missing 'name'"))
            continue

        if name in model_names:
            errors.append(
                ValidationError("error", src_path, f"duplicate model name '{name}'")
            )
        model_names.add(name)

        # table_reference vs ref_sql: exactly one required
        has_tref = bool(model.get("table_reference"))
        has_ref_sql = bool(model.get("ref_sql"))
        if has_tref and has_ref_sql:
            errors.append(
                ValidationError(
                    "error",
                    f"{src_path} > {name}",
                    "model has both 'table_reference' and 'ref_sql' — choose one",
                )
            )
        elif not has_tref and not has_ref_sql:
            errors.append(
                ValidationError(
                    "error",
                    f"{src_path} > {name}",
                    "model must define either 'table_reference' or 'ref_sql'",
                )
            )
        elif has_tref:
            tref = model.get("table_reference", {})
            if not tref.get("table"):
                errors.append(
                    ValidationError(
                        "warning",
                        f"{src_path} > {name}",
                        "table_reference.table is empty",
                    )
                )

        columns = model.get("columns", [])
        if not isinstance(columns, list):
            errors.append(
                ValidationError(
                    "error", f"{src_path} > {name}", "columns must be a list"
                )
            )
            columns = []
        if not columns:
            errors.append(
                ValidationError(
                    "warning", f"{src_path} > {name}", "model has no columns"
                )
            )

        col_names = set()
        for j, col in enumerate(columns):
            if not isinstance(col, dict):
                errors.append(
                    ValidationError(
                        "error",
                        f"{src_path} > {name} > columns[{j}]",
                        "column entry must be an object",
                    )
                )
                continue
            col_name = col.get("name")
            if not col_name:
                errors.append(
                    ValidationError(
                        "error",
                        f"{src_path} > {name} > columns[{j}]",
                        "column missing 'name'",
                    )
                )
                continue
            if col_name in col_names:
                errors.append(
                    ValidationError(
                        "error",
                        f"{src_path} > {name}",
                        f"duplicate column '{col_name}'",
                    )
                )
            col_names.add(col_name)

            if not col.get("type"):
                errors.append(
                    ValidationError(
                        "warning",
                        f"{src_path} > {name} > {col_name}",
                        "column missing 'type'",
                    )
                )

        pk = model.get("primary_key")
        if pk and pk not in col_names:
            errors.append(
                ValidationError(
                    "error",
                    f"{src_path} > {name}",
                    f"primary_key '{pk}' not found in columns",
                )
            )

    # Check views
    for i, view in enumerate(views):
        src_dir = view.get("_source_dir", f"views[{i}]")
        name = view.get("name")
        if not name:
            errors.append(
                ValidationError(
                    "error", f"views/{src_dir}/metadata.yml", "view missing 'name'"
                )
            )
            continue
        if name in view_names or name in model_names:
            errors.append(
                ValidationError("error", f"views/{src_dir}", f"duplicate name '{name}'")
            )
        view_names.add(name)

        if not view.get("statement"):
            errors.append(
                ValidationError(
                    "error",
                    f"views/{src_dir}",
                    "view missing 'statement' (define in metadata.yml or sql.yml)",
                )
            )

    # Check relationships
    all_entity_names = model_names | view_names
    for i, rel in enumerate(relationships):
        if not isinstance(rel, dict):
            errors.append(
                ValidationError(
                    "error",
                    f"relationships[{i}]",
                    "relationship entry must be an object",
                )
            )
            continue
        rel_name = rel.get("name", f"relationships[{i}]")
        ref_models = rel.get("models", [])
        for m in ref_models:
            if m not in all_entity_names:
                errors.append(
                    ValidationError(
                        "error",
                        f"relationships > {rel_name}",
                        f"references unknown model '{m}'",
                    )
                )
        if not rel.get("condition"):
            errors.append(
                ValidationError(
                    "warning", f"relationships > {rel_name}", "missing join condition"
                )
            )
        if not rel.get("join_type"):
            errors.append(
                ValidationError(
                    "warning", f"relationships > {rel_name}", "missing join_type"
                )
            )

    return errors
