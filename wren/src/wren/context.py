"""Context management — load YAML MDL files, validate, build manifest JSON."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
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


# Known camelCase → snake_case pairs (inverse of snake→camel mapping)
_CAMEL_TO_SNAKE_MAP = {
    "tableReference": "table_reference",
    "refSql": "ref_sql",
    "isCalculated": "is_calculated",
    "notNull": "not_null",
    "isPrimaryKey": "is_primary_key",
    "primaryKey": "primary_key",
    "joinType": "join_type",
    "dataSource": "data_source",
}


def _camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case.

    Uses known mapping table first, then generic regex fallback.
    """
    if name in _CAMEL_TO_SNAKE_MAP:
        return _CAMEL_TO_SNAKE_MAP[name]
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def _convert_keys_to_snake(obj: Any) -> Any:
    """Recursively convert all dict keys from camelCase to snake_case."""
    if isinstance(obj, dict):
        return {_camel_to_snake(k): _convert_keys_to_snake(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_keys_to_snake(item) for item in obj]
    return obj


# ── MDL JSON → YAML project conversion ───────────────────────────────────


@dataclass
class ProjectFile:
    """A file to be written during project conversion."""

    relative_path: str  # e.g. "models/orders/metadata.yml"
    content: str  # file content (YAML or SQL or Markdown)


def convert_mdl_to_project(mdl_json: dict) -> list[ProjectFile]:
    """Convert an MDL JSON manifest to a list of project files.

    Args:
        mdl_json: Parsed MDL JSON (camelCase keys).

    Returns:
        List of ProjectFile objects, each representing a file to write.
    """
    files: list[ProjectFile] = []

    # ── wren_project.yml ──────────────────────────────────────
    project_config: dict[str, Any] = {"schema_version": 2}
    if "name" in mdl_json:
        project_config["name"] = mdl_json["name"]
    elif "projectName" in mdl_json:
        project_config["name"] = mdl_json["projectName"]
    if "catalog" in mdl_json:
        project_config["catalog"] = mdl_json["catalog"]
    if "schema" in mdl_json:
        project_config["schema"] = mdl_json["schema"]
    if "dataSource" in mdl_json:
        project_config["data_source"] = mdl_json["dataSource"]

    files.append(
        ProjectFile(
            relative_path="wren_project.yml",
            content=yaml.dump(
                project_config, default_flow_style=False, sort_keys=False
            ),
        )
    )

    # ── Models ────────────────────────────────────────────────
    for i, model in enumerate(mdl_json.get("models", [])):
        model_snake = _convert_keys_to_snake(model)
        if "name" not in model_snake:
            raise ValueError(f"Model at index {i} is missing required 'name' field")
        name = model_snake["name"]
        dir_path = f"models/{name}"

        ref_sql = model_snake.pop("ref_sql", None)
        if ref_sql:
            files.append(
                ProjectFile(
                    relative_path=f"{dir_path}/ref_sql.sql",
                    content=ref_sql.strip() + "\n",
                )
            )

        files.append(
            ProjectFile(
                relative_path=f"{dir_path}/metadata.yml",
                content=yaml.dump(
                    model_snake, default_flow_style=False, sort_keys=False
                ),
            )
        )

    # ── Views ─────────────────────────────────────────────────
    for i, view in enumerate(mdl_json.get("views", [])):
        view_snake = _convert_keys_to_snake(view)
        if "name" not in view_snake:
            raise ValueError(f"View at index {i} is missing required 'name' field")
        name = view_snake["name"]
        dir_path = f"views/{name}"

        statement = view_snake.pop("statement", None)
        if statement and "\n" in statement.strip():
            files.append(
                ProjectFile(
                    relative_path=f"{dir_path}/sql.yml",
                    content=yaml.dump(
                        {"statement": statement},
                        default_flow_style=False,
                        sort_keys=False,
                    ),
                )
            )
        elif statement:
            view_snake["statement"] = statement

        files.append(
            ProjectFile(
                relative_path=f"{dir_path}/metadata.yml",
                content=yaml.dump(
                    view_snake, default_flow_style=False, sort_keys=False
                ),
            )
        )

    # ── Relationships ─────────────────────────────────────────
    relationships = mdl_json.get("relationships", [])
    if relationships:
        rels_snake = [_convert_keys_to_snake(r) for r in relationships]
        files.append(
            ProjectFile(
                relative_path="relationships.yml",
                content=yaml.dump(
                    {"relationships": rels_snake},
                    default_flow_style=False,
                    sort_keys=False,
                ),
            )
        )

    # ── Instructions ──────────────────────────────────────────
    instructions = mdl_json.get("_instructions")
    if instructions:
        files.append(
            ProjectFile(
                relative_path="instructions.md",
                content=instructions.strip() + "\n",
            )
        )

    return files


def write_project_files(
    files: list[ProjectFile],
    output_dir: Path,
    *,
    force: bool = False,
) -> None:
    """Write project files to disk.

    Args:
        files: List of ProjectFile from convert_mdl_to_project().
        output_dir: Target directory.
        force: If False, raise SystemExit if wren_project.yml already exists.
    """
    output_dir = Path(output_dir)
    project_file = output_dir / "wren_project.yml"

    if force and output_dir.exists():
        import shutil  # noqa: PLC0415

        for managed in (
            "models",
            "views",
            "relationships.yml",
            "instructions.md",
            "wren_project.yml",
        ):
            target = output_dir / managed
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()

    if project_file.exists() and not force:
        raise SystemExit(
            f"Error: {project_file} already exists. Use --force to overwrite."
        )

    for f in files:
        root = output_dir.resolve()
        path = (output_dir / f.relative_path).resolve()
        try:
            path.relative_to(root)
        except ValueError:
            raise SystemExit(f"Error: invalid output path: {f.relative_path!r}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f.content)


# ── Project discovery ─────────────────────────────────────────────────────


def load_global_config() -> dict:
    """Load ~/.wren/config.yml (global preferences)."""
    config_file = _WREN_HOME / "config.yml"
    if not config_file.exists():
        return {}
    return yaml.safe_load(config_file.read_text()) or {}


def discover_project_path(explicit: str | None = None) -> Path:
    """Return the project directory path.

    Priority:
    1. explicit arg (--project / --path flag)
    2. WREN_PROJECT_HOME env var
    3. Walk up from cwd looking for wren_project.yml
    4. default_project in ~/.wren/config.yml
    5. Raise SystemExit with actionable message
    """
    if explicit:
        return Path(explicit).expanduser()

    # 2. WREN_PROJECT_HOME env var
    env = os.environ.get("WREN_PROJECT_HOME")
    if env:
        return Path(env).expanduser()

    # 3. Walk up from cwd looking for wren_project.yml
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / _PROJECT_FILE).exists():
            return parent
        # Stop at home or root
        if parent == Path.home() or parent == parent.parent:
            break

    # 4. ~/.wren/config.yml default_project
    cfg = load_global_config()
    if cfg.get("default_project"):
        return Path(cfg["default_project"]).expanduser()

    raise SystemExit(
        "Error: no wren project found.\n"
        "  Run `wren context init` to create one, or set WREN_PROJECT_HOME."
    )


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
    Use build_json() to get the camelCase JSON form for the engine.
    Instructions are not included — use load_instructions() separately.
    """
    project_config = load_project_config(project_path)
    models = load_models(project_path)
    views = load_views(project_path)
    relationships = load_relationships(project_path)

    # Strip internal metadata
    for m in models:
        m.pop("_source_dir", None)
    for v in views:
        v.pop("_source_dir", None)

    return {
        "catalog": project_config.get("catalog", "wren"),
        "schema": project_config.get("schema", "public"),
        "models": models,
        "relationships": relationships,
        "views": views,
    }


def build_json(project_path: Path) -> dict:
    """Build the final camelCase JSON manifest for the engine."""
    return _convert_keys(build_manifest(project_path))


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

    if any(e.path == _PROJECT_FILE and "schema_version" in e.message for e in errors):
        return errors

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


# ── Semantic validation (view dry-plan + description completeness) ─────────

_VALID_LEVELS = frozenset({"error", "warning", "strict"})


def _prop_description(item: dict) -> str | None:
    return (item.get("properties") or {}).get("description")


def _check_descriptions(manifest: dict, *, strict: bool = False) -> list[str]:
    warnings: list[str] = []

    for model in manifest.get("models", []):
        name = model.get("name", "<unknown>")
        if not _prop_description(model):
            warnings.append(
                f"Model '{name}' has no description — "
                "add properties.description to improve memory search and agent comprehension"
            )
        if strict:
            for col in model.get("columns", []):
                col_name = col.get("name", "<unknown>")
                if not _prop_description(col):
                    warnings.append(
                        f"Column '{col_name}' in model '{name}' has no description"
                    )

    for view in manifest.get("views", []):
        view_name = view.get("name", "<unknown>")
        if not _prop_description(view):
            warnings.append(
                f"View '{view_name}' has no description — "
                "views with descriptions are indexed as NL-SQL examples in memory"
            )

    return warnings


def validate_manifest(
    manifest_str: str,
    data_source: str,
    *,
    level: str = "warning",
) -> dict:
    """Semantic validation of a compiled MDL manifest (base64-encoded JSON).

    Args:
        manifest_str: Base64-encoded MDL JSON.
        data_source: Target data source (used for view dry-plan dialect).
        level: Validation level.
            "error"   — view SQL dry-plan only (CI/CD)
            "warning" — + model/view missing description (default)
            "strict"  — + column missing description

    Returns:
        Dict with "errors" (list) and "warnings" (list).
    """
    import base64 as _base64  # noqa: PLC0415

    from wren.engine import WrenEngine  # noqa: PLC0415
    from wren.model.data_source import DataSource  # noqa: PLC0415

    errors: list[str] = []
    warnings: list[str] = []

    if level not in _VALID_LEVELS:
        errors.append(
            f"Invalid level '{level}' — must be one of: {', '.join(sorted(_VALID_LEVELS))}"
        )
        return {"errors": errors, "warnings": warnings}

    try:
        manifest = json.loads(_base64.b64decode(manifest_str))
    except Exception as e:
        errors.append(f"Failed to decode manifest: {e}")
        return {"errors": errors, "warnings": warnings}

    # View SQL dry-plan — always checked (failures are errors)
    views = manifest.get("views", [])
    if views:
        if isinstance(data_source, str):
            try:
                data_source = DataSource(data_source)
            except ValueError:
                errors.append(f"Invalid datasource '{data_source}'")
                return {"errors": errors, "warnings": warnings}
        with WrenEngine(
            manifest_str=manifest_str, data_source=data_source, connection_info={}
        ) as engine:
            for view in views:
                name = view.get("name", "<unknown>")
                stmt = (view.get("statement") or "").strip()
                if not stmt:
                    errors.append(f"View '{name}': empty statement")
                    continue
                try:
                    engine.dry_plan(stmt)
                except Exception as e:
                    errors.append(f"View '{name}': dry-plan failed — {e}")

    # Description checks — only at warning/strict level
    if level in ("warning", "strict"):
        warnings.extend(_check_descriptions(manifest, strict=(level == "strict")))

    return {"errors": errors, "warnings": warnings}
