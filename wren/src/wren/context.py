"""MDL context validation — view SQL dry-plan + description completeness checks."""

from __future__ import annotations

import base64
import json

from wren.model.data_source import DataSource


def _prop_description(item: dict) -> str | None:
    return (item.get("properties") or {}).get("description")


def _check_descriptions(manifest: dict, *, strict: bool = False) -> list[str]:
    warnings: list[str] = []

    for model in manifest.get("models", []):
        if not _prop_description(model):
            warnings.append(
                f"Model '{model['name']}' has no description — "
                "add properties.description to improve memory search and agent comprehension"
            )
        if strict:
            for col in model.get("columns", []):
                if not _prop_description(col):
                    warnings.append(
                        f"Column '{col['name']}' in model '{model['name']}' has no description"
                    )

    for view in manifest.get("views", []):
        if not _prop_description(view):
            warnings.append(
                f"View '{view['name']}' has no description — "
                "views with descriptions are indexed as NL-SQL examples in memory"
            )

    return warnings


def validate(
    manifest_str: str,
    data_source: DataSource | str,
    *,
    level: str = "warning",
) -> dict:
    """Validate an MDL manifest.

    Args:
        manifest_str: Base64-encoded MDL JSON.
        data_source: Target data source (used for view dry-plan dialect).
        level: Validation level.
            "error"   — structural errors + view SQL dry-plan only (CI/CD)
            "warning" — + model/view missing description (default)
            "strict"  — + column missing description

    Returns:
        Dict with "errors" (list) and "warnings" (list).
    """
    from wren.engine import WrenEngine  # noqa: PLC0415

    errors: list[str] = []
    warnings: list[str] = []

    try:
        manifest = json.loads(base64.b64decode(manifest_str))
    except Exception as e:
        errors.append(f"Failed to decode manifest: {e}")
        return {"errors": errors, "warnings": warnings}

    # View SQL dry-plan — always checked (failures are errors)
    views = manifest.get("views", [])
    if views:
        if isinstance(data_source, str):
            data_source = DataSource(data_source)
        with WrenEngine(
            manifest_str=manifest_str, data_source=data_source, connection_info={}
        ) as engine:
            for view in views:
                name = view.get("name", "<unknown>")
                stmt = view.get("statement", "")
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
