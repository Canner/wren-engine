from __future__ import annotations

from pydantic import BaseModel, Field, SecretStr

manifest_str_field = Field(alias="manifestStr", description="Base64 manifest")
connection_info_field = Field(alias="connectionInfo")


class QueryDTO(BaseModel):
    sql: str
    manifest_str: str = manifest_str_field
    connection_info: ConnectionInfo = connection_info_field


class QueryBigQueryDTO(QueryDTO):
    connection_info: BigQueryConnectionInfo = connection_info_field


class QueryCannerDTO(QueryDTO):
    connection_info: ConnectionUrl | CannerConnectionInfo = connection_info_field


class QueryClickHouseDTO(QueryDTO):
    connection_info: ConnectionUrl | ClickHouseConnectionInfo = connection_info_field


class QueryMSSqlDTO(QueryDTO):
    connection_info: ConnectionUrl | MSSqlConnectionInfo = connection_info_field


class QueryMySqlDTO(QueryDTO):
    connection_info: ConnectionUrl | MySqlConnectionInfo = connection_info_field


class QueryPostgresDTO(QueryDTO):
    connection_info: ConnectionUrl | PostgresConnectionInfo = connection_info_field


class QuerySnowflakeDTO(QueryDTO):
    connection_info: SnowflakeConnectionInfo = connection_info_field


class QueryTrinoDTO(QueryDTO):
    connection_info: ConnectionUrl | TrinoConnectionInfo = connection_info_field


class BigQueryConnectionInfo(BaseModel):
    project_id: SecretStr
    dataset_id: SecretStr
    credentials: SecretStr = Field(description="Base64 encode `credentials.json`")


class CannerConnectionInfo(BaseModel):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretStr = Field(examples=[7432])
    user: SecretStr
    pat: SecretStr
    workspace: SecretStr


class ClickHouseConnectionInfo(BaseModel):
    host: SecretStr
    port: SecretStr
    database: SecretStr
    user: SecretStr
    password: SecretStr


class MSSqlConnectionInfo(BaseModel):
    host: SecretStr
    port: SecretStr
    database: SecretStr
    user: SecretStr
    password: SecretStr
    driver: str = Field(
        default="FreeTDS",
        description="On Mac and Linux this is usually `FreeTDS. On Windows, it is usually `ODBC Driver 18 for SQL Server`",
    )
    tds_version: str = Field(default="8.0", alias="TDS_Version")
    kwargs: dict[str, str] | None = Field(
        description="Additional keyword arguments to pass to PyODBC", default=None
    )


class MySqlConnectionInfo(BaseModel):
    host: SecretStr
    port: SecretStr
    database: SecretStr
    user: SecretStr
    password: SecretStr


class ConnectionUrl(BaseModel):
    connection_url: SecretStr = Field(alias="connectionUrl")


class PostgresConnectionInfo(BaseModel):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretStr = Field(examples=[5432])
    database: SecretStr
    user: SecretStr
    password: SecretStr


class SnowflakeConnectionInfo(BaseModel):
    user: SecretStr
    password: SecretStr
    account: SecretStr
    database: SecretStr
    sf_schema: SecretStr = Field(
        alias="schema"
    )  # Use `sf_schema` to avoid `schema` shadowing in BaseModel


class TrinoConnectionInfo(BaseModel):
    host: SecretStr
    port: SecretStr = Field(default="8080")
    catalog: SecretStr
    trino_schema: SecretStr = Field(
        alias="schema"
    )  # Use `trino_schema` to avoid `schema` shadowing in BaseModel
    user: SecretStr | None = None
    password: SecretStr | None = None


ConnectionInfo = (
    BigQueryConnectionInfo
    | CannerConnectionInfo
    | ConnectionUrl
    | MSSqlConnectionInfo
    | MySqlConnectionInfo
    | PostgresConnectionInfo
    | SnowflakeConnectionInfo
    | TrinoConnectionInfo
)


class ValidateDTO(BaseModel):
    manifest_str: str = manifest_str_field
    parameters: dict[str, str]
    connection_info: ConnectionInfo = connection_info_field


class AnalyzeSQLDTO(BaseModel):
    manifest_str: str = manifest_str_field
    sql: str


class AnalyzeSQLBatchDTO(BaseModel):
    manifest_str: str = manifest_str_field
    sqls: list[str]


class DryPlanDTO(BaseModel):
    manifest_str: str = manifest_str_field
    sql: str


class ConfigModel(BaseModel):
    diagnose: bool


class UnprocessableEntityError(Exception):
    pass


PG_TYPE_OID_NAMES = {
    1000: "_bool",
    1014: "_bpchar",
    1001: "_bytea",
    1002: "_char",
    1182: "_date",
    1021: "_float4",
    1022: "_float8",
    57645: "_hstore",
    1041: "_inet",
    1005: "_int2",
    1007: "_int4",
    1016: "_int8",
    199: "_json",
    1003: "_name",
    1231: "_numeric",
    1028: "_oid",
    2287: "_record",
    2210: "_regclass",
    4090: "_regnamespace",
    2208: "_regoper",
    2209: "_regoperator",
    1008: "_regproc",
    2207: "_regprocedure",
    2211: "_regtype",
    1009: "_text",
    1115: "_timestamp",
    1185: "_timestamptz",
    2951: "_uuid",
    1015: "_varchar",
    1011: "_xid",
    2276: "any",
    16: "bool",
    1042: "bpchar",
    17: "bytea",
    18: "char",
    1082: "date",
    700: "float4",
    701: "float8",
    57640: "hstore",
    869: "inet",
    21: "int2",
    23: "int4",
    20: "int8",
    114: "json",
    19: "name",
    1700: "numeric",
    26: "oid",
    2249: "record",
    2205: "regclass",
    4089: "regnamespace",
    2203: "regoper",
    2204: "regoperator",
    24: "regproc",
    2022: "regprocedure",
    2206: "regtype",
    25: "text",
    1114: "timestamp",
    1184: "timestamptz",
    2279: "trigger",
    2950: "uuid",
    1043: "varchar",
    28: "xid",
}
