"""Generate canonical NL-SQL seed pairs from an MDL manifest.

Pure functions — no LanceDB or embedding dependency.
"""

from __future__ import annotations

_NUMERIC_TYPES = {
    "int",
    "integer",
    "bigint",
    "smallint",
    "tinyint",
    "float",
    "double",
    "decimal",
    "numeric",
    "real",
    "number",
}

SEED_TAG = "source:seed"


def generate_seed_queries(manifest: dict) -> list[dict]:
    """Return a list of {"nl": ..., "sql": ...} seed pairs."""
    pairs: list[dict] = []

    for model in manifest.get("models", []):
        pairs.extend(_model_seeds(model))

    for rel in manifest.get("relationships", []):
        pair = _relationship_seed(rel)
        if pair:
            pairs.append(pair)

    return pairs


def _model_seeds(model: dict) -> list[dict]:
    name = model["name"]
    columns = model.get("columns", [])
    pairs = []

    # Template 1: basic listing
    pairs.append(
        {
            "nl": f"List all {name}",
            "sql": f"SELECT * FROM {name} LIMIT 100",
        }
    )

    # Find first numeric column (non-calculated) and first groupable column
    numeric_col = None
    group_col = None
    for col in columns:
        col_type = (col.get("type") or "").split("(")[0].lower().strip()
        is_calc = col.get("isCalculated", False)
        is_pk = col["name"] == model.get("primaryKey")

        if (
            col_type in _NUMERIC_TYPES
            and not is_calc
            and not is_pk
            and numeric_col is None
        ):
            numeric_col = col["name"]
        elif (
            col_type not in _NUMERIC_TYPES
            and not is_pk
            and not is_calc
            and group_col is None
        ):
            group_col = col["name"]

    # Template 2a: simple aggregation
    if numeric_col:
        pairs.append(
            {
                "nl": f"Total {numeric_col} in {name}",
                "sql": f"SELECT SUM({numeric_col}) FROM {name}",
            }
        )

    # Template 2b: grouped aggregation
    if numeric_col and group_col:
        pairs.append(
            {
                "nl": f"{numeric_col} by {group_col} in {name}",
                "sql": f"SELECT {group_col}, SUM({numeric_col}) FROM {name} GROUP BY 1",
            }
        )

    return pairs


def _relationship_seed(rel: dict) -> dict | None:
    models = rel.get("models", [])
    condition = rel.get("condition", "").strip()

    if len(models) < 2 or not condition:
        return None

    left, right = models[0], models[1]

    return {
        "nl": f"{left} with {right} details",
        "sql": f"SELECT * FROM {left} JOIN {right} ON {condition} LIMIT 100",
    }
