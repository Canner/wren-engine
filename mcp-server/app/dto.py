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
    threshold: ColumnLevelAccessControlThreshold | None = None


class Column(BaseModel):
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
    mdl_schema: str | None = Field(alias="schema", default=None)
    table: str


class RowLevelAccessControl(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    requiredProperties: list[SessionProperty]
    condition: str


class Model(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    tableReference: TableReference | None = None
    refSql: str | None = None
    baseObject: str | None = None
    columns: list[Column] | None = None
    primaryKey: str | None = None
    cached: bool | None = None
    refreshTime: str | None = None
    rowLevelAccessControls: list[RowLevelAccessControl] | None = None
    properties: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_table_source(self) -> Model:
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


class Relationship(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    models: list[str] = Field(min_length=2, max_length=2)
    joinType: Literal["ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY"]
    condition: str
    properties: dict[str, Any] | None = None


class View(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    statement: str
    properties: dict[str, Any] | None = None


class TimeGrain(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    refColumn: str
    dateParts: list[
        Literal["YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"]
    ]


class Metric(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    baseObject: str
    dimension: list[Column]
    measure: list[Column] = Field(min_length=1)
    timeGrain: list[TimeGrain] | None = None
    cached: bool | None = None
    refreshTime: str | None = None
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


class Manifest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str = "wren"
    mdl_schema: str = Field(alias="schema", default="public")
    dataSource: DataSourceType | None = None
    sampleDataFolder: str | None = None
    models: list[Model] | None = None
    relationships: list[Relationship] | None = None
    views: list[View] | None = None
    metrics: list[Metric] | None = None
    enumDefinitions: list[EnumDefinition] | None = None


class TableColumns(BaseModel):
    table_name: str
    column_names: list[str] | None = None
