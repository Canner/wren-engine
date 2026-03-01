"""Pydantic models for Wren MDL manifest.

Corresponds to the official schema:
https://raw.githubusercontent.com/Canner/WrenAI/main/wren-mdl/mdl.schema.json
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SessionProperty(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    required: bool
    defaultExpr: str | None = None


class ColumnLevelAccessControlThreshold(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    value: str
    dataType: Literal["NUMERIC", "STRING"]


class ColumnLevelAccessControl(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    operator: Literal[
        "EQUALS",
        "NOT_EQUALS",
        "GREATER_THAN",
        "LESS_THAN",
        "GREATER_THAN_OR_EQUALS",
        "LESS_THAN_OR_EQUALS",
    ]
    requiredProperties: list[SessionProperty] = Field(min_length=1, max_length=1)
    threshold: ColumnLevelAccessControlThreshold


class MDLColumn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    type: str
    relationship: str | None = None
    isCalculated: bool | None = None
    notNull: bool | None = None
    expression: str | None = None
    isHidden: bool | None = None
    columnLevelAccessControl: ColumnLevelAccessControl | None = None
    properties: dict[str, Any] | None = None


class TableReference(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str | None = None
    # Named schema_name internally; alias "schema" matches MDL JSON key.
    schema_name: str | None = Field(default=None, alias="schema")
    table: str


class RowLevelAccessControl(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    requiredProperties: list[SessionProperty]
    condition: str


class MDLModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    tableReference: TableReference | None = None
    refSql: str | None = None
    baseObject: str | None = None
    columns: list[MDLColumn] | None = None
    primaryKey: str | None = None
    cached: bool | None = None
    refreshTime: str | None = None
    rowLevelAccessControls: list[RowLevelAccessControl] | None = None
    properties: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_table_source(self) -> MDLModel:
        sources = [
            x
            for x in [self.tableReference, self.refSql, self.baseObject]
            if x is not None
        ]
        if len(sources) != 1:
            raise ValueError(
                "Exactly one of tableReference, refSql, or baseObject must be provided"
            )
        return self


class MDLRelationship(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    models: list[str] = Field(min_length=2, max_length=2)
    joinType: Literal["ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY"]
    condition: str
    properties: dict[str, Any] | None = None


class TimeGrain(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    refColumn: str
    dateParts: list[
        Literal["YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"]
    ]


class MDLMetric(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    baseObject: str
    dimension: list[MDLColumn]
    measure: list[MDLColumn] = Field(min_length=1)
    timeGrain: list[TimeGrain] | None = None
    cached: bool | None = None
    refreshTime: str | None = None
    properties: dict[str, Any] | None = None


class MDLView(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    statement: str
    properties: dict[str, Any] | None = None


class EnumMember(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    value: str | None = None
    properties: dict[str, Any] | None = None


class EnumDefinition(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    values: list[EnumMember]
    properties: dict[str, Any] | None = None


DataSourceType = Literal[
    "BIGQUERY",
    "CLICKHOUSE",
    "CANNER",
    "TRINO",
    "MSSQL",
    "MYSQL",
    "POSTGRES",
    "SNOWFLAKE",
    "DUCKDB",
    "LOCAL_FILE",
    "S3_FILE",
    "GCS_FILE",
    "MINIO_FILE",
    "ORACLE",
    "ATHENA",
    "REDSHIFT",
]


class MDLManifest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str
    schema_name: str = Field(alias="schema")
    dataSource: DataSourceType | None = None
    sampleDataFolder: str | None = None
    models: list[MDLModel] | None = None
    relationships: list[MDLRelationship] | None = None
    metrics: list[MDLMetric] | None = None
    views: list[MDLView] | None = None
    enumDefinitions: list[EnumDefinition] | None = None

    def to_mdl_dict(self) -> dict[str, Any]:
        """Serialize to MDL-compatible dict (uses 'schema' key, not 'schema_name')."""
        return self.model_dump(by_alias=True, exclude_none=True)


class AgentQuestion(BaseModel):
    """Returned when the agent needs more information from the user."""

    question: str
