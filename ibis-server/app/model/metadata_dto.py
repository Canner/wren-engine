from enum import Enum
from app.model.data_source import (
    PostgresConnectionUrl,
    PostgresConnectionInfo,
    BigQueryConnectionInfo,
    SnowflakeConnectionInfo,
)
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union


class MetadataDTO(BaseModel):
    connection_info: Union[
        PostgresConnectionUrl | PostgresConnectionInfo,
        BigQueryConnectionInfo,
        SnowflakeConnectionInfo,
    ] = Field(alias="connectionInfo")


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


class CompactColumn(BaseModel):
    name: str
    type: str
    notNull: bool
    description: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class CompactTableProperties(BaseModel):
    schema: Optional[str]
    catalog: Optional[str]
    table: Optional[str]  # only table name without schema or catalog


class CompactTable(BaseModel):
    name: str  # unique table name (might contain schema name or catalog name as well)
    columns: List[CompactColumn]
    description: Optional[str] = None
    properties: CompactTableProperties = None
    primaryKey: Optional[str] = None


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
