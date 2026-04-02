"""Generate documentation for Wren connection info models."""

from __future__ import annotations

import json
from typing import Any, Union

from pydantic import SecretStr

from wren.model import (
    AthenaConnectionInfo,
    BaseConnectionInfo,
    BigQueryDatasetConnectionInfo,
    BigQueryProjectConnectionInfo,
    CannerConnectionInfo,
    ClickHouseConnectionInfo,
    ConnectionUrl,
    DatabricksServicePrincipalConnectionInfo,
    DatabricksTokenConnectionInfo,
    DorisConnectionInfo,
    GcsFileConnectionInfo,
    LocalFileConnectionInfo,
    MinioFileConnectionInfo,
    MSSqlConnectionInfo,
    MySqlConnectionInfo,
    OracleConnectionInfo,
    PostgresConnectionInfo,
    RedshiftConnectionInfo,
    RedshiftIAMConnectionInfo,
    S3FileConnectionInfo,
    SnowflakeConnectionInfo,
    SparkConnectionInfo,
    TrinoConnectionInfo,
)

# Mapping from DataSource name → list of ConnectionInfo classes.
# Sources with discriminated unions list all variants.
DATASOURCE_MODELS: dict[str, list[type[BaseConnectionInfo]]] = {
    "athena": [AthenaConnectionInfo],
    "bigquery": [BigQueryDatasetConnectionInfo, BigQueryProjectConnectionInfo],
    "canner": [CannerConnectionInfo],
    "clickhouse": [ClickHouseConnectionInfo],
    "databricks": [
        DatabricksTokenConnectionInfo,
        DatabricksServicePrincipalConnectionInfo,
    ],
    "doris": [DorisConnectionInfo],
    "duckdb": [LocalFileConnectionInfo],
    "gcs_file": [GcsFileConnectionInfo],
    "local_file": [LocalFileConnectionInfo],
    "minio_file": [MinioFileConnectionInfo],
    "mssql": [MSSqlConnectionInfo],
    "mysql": [MySqlConnectionInfo],
    "oracle": [OracleConnectionInfo],
    "postgres": [PostgresConnectionInfo],
    "redshift": [RedshiftConnectionInfo, RedshiftIAMConnectionInfo],
    "s3_file": [S3FileConnectionInfo],
    "snowflake": [SnowflakeConnectionInfo],
    "spark": [SparkConnectionInfo],
    "trino": [TrinoConnectionInfo],
    "connection_url": [ConnectionUrl],
}


def _union_args(annotation) -> tuple | None:
    """Return the type args if annotation is a Union/UnionType, else None."""
    import types  # noqa: PLC0415

    if isinstance(annotation, types.UnionType):
        return annotation.__args__
    origin = getattr(annotation, "__origin__", None)
    if origin is Union:
        return annotation.__args__
    return None


def _is_sensitive(field_info) -> bool:
    """Check if a field uses SecretStr (i.e. holds sensitive data)."""
    annotation = field_info.annotation
    args = _union_args(annotation)
    if args:
        return any(a is SecretStr for a in args)
    return annotation is SecretStr


def _friendly_type(annotation) -> str:
    """Convert a single type annotation to a readable string."""
    if annotation is SecretStr:
        return "string"
    if annotation is bool:
        return "boolean"
    if annotation is int:
        return "integer"
    if annotation is str:
        return "string"
    # dict[str, str] etc.
    origin = getattr(annotation, "__origin__", None)
    if origin is dict:
        return "object"
    if origin is list:
        return "array"
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def _type_label(field_info) -> str:
    """Return a human-readable type label for a field."""
    annotation = field_info.annotation
    args = _union_args(annotation)
    if args:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _friendly_type(non_none[0])
        return " | ".join(_friendly_type(a) for a in non_none)
    return _friendly_type(annotation)


def _field_default(field_info) -> str:
    """Return a display string for the field's default value."""
    if field_info.is_required():
        return ""
    default = field_info.default
    if default is None:
        return "null"
    if isinstance(default, SecretStr):
        return f'"{default.get_secret_value()}"'
    if isinstance(default, bool):
        return str(default).lower()
    if isinstance(default, str):
        return f'"{default}"'
    return str(default)


def _format_model_markdown(model: type[BaseConnectionInfo]) -> str:
    """Format a single ConnectionInfo model as a Markdown section."""
    lines: list[str] = []
    lines.append(f"### {model.__name__}")
    lines.append("")

    # Build table
    lines.append("| Field | Type | Required | Default | Sensitive | Alias | Example |")
    lines.append("|-------|------|----------|---------|-----------|-------|---------|")

    for name, field_info in model.model_fields.items():
        type_label = _type_label(field_info)
        required = "yes" if field_info.is_required() else "no"
        default = _field_default(field_info)
        sensitive = "yes" if _is_sensitive(field_info) else "no"
        alias = (
            field_info.alias if field_info.alias and field_info.alias != name else ""
        )
        examples = field_info.examples or []
        example_str = ", ".join(f"`{e}`" for e in examples)
        lines.append(
            f"| `{name}` | {type_label} | {required} | {default} | {sensitive} | {alias} | {example_str} |"
        )

    lines.append("")

    # JSON example
    example = _build_example(model)
    if example:
        lines.append("**Example:**")
        lines.append("```json")
        lines.append(json.dumps(example, indent=2))
        lines.append("```")
        lines.append("")

    return "\n".join(lines)


def _build_example(model: type[BaseConnectionInfo]) -> dict[str, Any]:
    """Build an example JSON dict from field metadata."""
    example: dict[str, Any] = {}
    for name, field_info in model.model_fields.items():
        key = (
            field_info.alias if field_info.alias and field_info.alias != name else name
        )
        if field_info.examples:
            example[key] = field_info.examples[0]
        elif not field_info.is_required():
            continue  # skip optional fields without examples
        else:
            example[key] = f"<{name}>"
    return example


def generate_markdown(datasource: str | None = None) -> str:
    """Generate Markdown documentation for connection info models.

    Args:
        datasource: If given, only generate docs for that data source.
                    If None, generate for all data sources.
    """
    lines: list[str] = []
    lines.append("# Wren Engine Connection Info Reference")
    lines.append("")

    if datasource:
        key = datasource.lower()
        if key not in DATASOURCE_MODELS:
            return f"Unknown data source: {datasource}\nAvailable: {', '.join(sorted(DATASOURCE_MODELS))}"
        sources = {key: DATASOURCE_MODELS[key]}
    else:
        sources = DATASOURCE_MODELS

    for ds_name, models in sources.items():
        lines.append(f"## {ds_name}")
        lines.append("")
        for model in models:
            lines.append(_format_model_markdown(model))

    return "\n".join(lines)


def _generate_raw_json_schema(datasource: str | None = None) -> str:
    """Generate raw JSON Schema for connection info models."""
    if datasource:
        key = datasource.lower()
        if key not in DATASOURCE_MODELS:
            return json.dumps(
                {
                    "error": f"Unknown data source: {datasource}",
                    "available": sorted(DATASOURCE_MODELS.keys()),
                },
                indent=2,
            )
        sources = {key: DATASOURCE_MODELS[key]}
    else:
        sources = DATASOURCE_MODELS

    schemas: dict[str, Any] = {}
    for ds_name, models in sources.items():
        if len(models) == 1:
            schemas[ds_name] = models[0].model_json_schema()
        else:
            schemas[ds_name] = {
                "variants": {m.__name__: m.model_json_schema() for m in models}
            }

    return json.dumps(schemas, indent=2)


def generate_json_schema(
    datasource: str | None = None, *, envelope: bool = False
) -> str:
    """Generate JSON Schema for connection info models.

    Args:
        datasource: If given, only generate schema for that data source.
                    If None, generate for all data sources.
        envelope: If True, wrap output in ``{"datasource": ..., "properties": ...}``
                  envelope format (one object per data source).
    """
    if not envelope:
        return _generate_raw_json_schema(datasource)

    if datasource:
        key = datasource.lower()
        if key not in DATASOURCE_MODELS:
            return json.dumps(
                {
                    "error": f"Unknown data source: {datasource}",
                    "available": sorted(DATASOURCE_MODELS.keys()),
                },
                indent=2,
            )
        sources = {key: DATASOURCE_MODELS[key]}
    else:
        sources = DATASOURCE_MODELS

    results: list[dict[str, Any]] = []
    for ds_name, models in sources.items():
        for model in models:
            example = _build_example(model)
            results.append({"datasource": ds_name, "properties": example})

    if len(results) == 1:
        return json.dumps(results[0], indent=2)
    return json.dumps(results, indent=2)
