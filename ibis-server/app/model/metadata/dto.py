from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from app.model import ConnectionInfo


class MetadataDTO(BaseModel):
    connection_info: ConnectionInfo = Field(alias="connectionInfo")


class WrenEngineColumnType(Enum):
    # Boolean Types
    BOOLEAN = "BOOLEAN"

    # Numeric Types
    TINYINT = "TINYINT"
    INT2 = "INT2"
    SMALLINT = "SMALLINT"  # alias for INT2
    INT4 = "INT4"
    INTEGER = "INTEGER"  # alias for INT4
    INT8 = "INT8"
    BIGINT = "BIGINT"  # alias for INT8
    NUMERIC = "NUMERIC"
    DECIMAL = "DECIMAL"

    # Floating-Point Types
    FLOAT4 = "FLOAT4"
    REAL = "REAL"  # alias for FLOAT4
    FLOAT8 = "FLOAT8"
    DOUBLE = "DOUBLE"  # alias for FLOAT8

    # Character Types
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    BPCHAR = "BPCHAR"  # BPCHAR is fixed-length blank padded string
    TEXT = "TEXT"  # alias for VARCHAR
    STRING = "STRING"  # alias for VARCHAR
    NAME = "NAME"  # alias for VARCHAR

    # Date/Time Types
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"
    DATE = "DATE"
    INTERVAL = "INTERVAL"

    # JSON Types
    JSON = "JSON"

    # Object identifiers (OIDs) are used internally by PostgreSQL as primary keys for various system tables.
    # https:#www.postgresql.org/docs/current/datatype-oid.html
    OID = "OID"

    # Binary Data Types
    BYTEA = "BYTEA"

    # UUID Type
    UUID = "UUID"

    # Network Address Types
    INET = "INET"

    # Unknown Type
    UNKNOWN = "UNKNOWN"


class Column(BaseModel):
    name: str
    type: str
    notNull: bool
    description: str | None = None
    properties: dict[str, Any] | None = None


class TableProperties(BaseModel):
    schema: str | None
    catalog: str | None
    table: str | None  # only table name without schema or catalog


class Table(BaseModel):
    name: str  # unique table name (might contain schema name or catalog name as well)
    columns: list[Column]
    description: str | None = None
    properties: TableProperties = None
    primaryKey: str | None = None


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
