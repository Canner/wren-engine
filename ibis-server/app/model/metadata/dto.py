from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from app.model import ConnectionInfo
from app.model.data_source import DataSource


class V2MetadataDTO(BaseModel):
    connection_info: dict[str, Any] | ConnectionInfo = Field(alias="connectionInfo")


class FilterInfo(BaseModel):
    pass


class MetadataDTO(BaseModel):
    connection_info: dict[str, Any] | ConnectionInfo = Field(alias="connectionInfo")
    table_limit: int | None = Field(alias="limit", default=None)
    filter_info: dict[str, Any] | None = Field(alias="filterInfo", default=None)


class BigQueryFilterInfo(FilterInfo):
    projects: list["ProjectDatasets"] | None = None


class ProjectDatasets(BaseModel):
    project_id: str = Field(alias="projectId")
    dataset_ids: list[str] | None = Field(alias="datasetIds", default=None)


def get_filter_info(data_source: DataSource, info: dict[str, Any]) -> FilterInfo | None:
    if data_source == DataSource.bigquery:
        return BigQueryFilterInfo(**info)
    return None


class RustWrenEngineColumnType(Enum):
    BOOL = "BOOL"
    TINYINT = "TINYINT"
    INT2 = "INT2"
    SMALLINT = "SMALLINT"
    INT4 = "INT4"
    INT = "INT"
    INTEGER = "INTEGER"
    INT8 = "INT8"
    BIGINT = "BIGINT"
    NUMERIC = "NUMERIC"
    DECIMAL = "DECIMAL"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    BPCHAR = "BPCHAR"
    TEXT = "TEXT"
    STRING = "STRING"
    NAME = "NAME"
    FLOAT4 = "FLOAT4"
    REAL = "REAL"
    FLOAT = "FLOAT"
    FLOAT8 = "FLOAT8"
    DOUBLE = "DOUBLE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    DATE = "DATE"
    INTERVAL = "INTERVAL"
    JSON = "JSON"
    OID = "OID"
    BYTEA = "BYTEA"
    UUID = "UUID"
    INET = "INET"
    UNKNOWN = "UNKNOWN"
    BIGNUMERIC = "BIGNUMERIC"
    BYTES = "BYTES"
    DATETIME = "DATETIME"
    FLOAT64 = "FLOAT64"
    INT64 = "INT64"
    TIME = "TIME"
    NULL = "NULL"
    VARIANT = "VARIANT"

    # Extension types
    ## PostGIS
    GEOMETRY = "GEOMETRY"
    GEOGRAPHY = "GEOGRAPHY"


class Column(BaseModel):
    name: str
    type: str
    notNull: bool
    description: str | None = None
    properties: dict[str, Any] | None = None
    nestedColumns: list["Column"] | None = None


class TableProperties(BaseModel):
    # To prevent schema shadowing in Pydantic, avoid using schema as a field name
    schema_: str | None = Field(alias="schema", default=None)
    catalog: str | None
    table: str | None  # only table name without schema or catalog
    path: str | None = Field(
        alias="path", default=None
    )  # the full path of the table for file-based table


class Table(BaseModel):
    name: str  # unique table name (might contain schema name or catalog name as well)
    columns: list[Column]
    description: str | None = None
    properties: TableProperties = None
    primaryKey: str | None = None


class Catalog(BaseModel):
    name: str
    schemas: list[str]


class ConstraintType(Enum):
    PRIMARY_KEY = "PRIMARY KEY"
    FOREIGN_KEY = "FOREIGN KEY"
    UNIQUE = "UNIQUE"


class Constraint(BaseModel):
    constraintName: str
    constraintType: ConstraintType
    constraintTable: str
    constraintColumn: str
    constraintedTable: str
    constraintedColumn: str
