from __future__ import annotations

from pydantic import BaseModel, Field

manifest_str_field = Field(alias="manifestStr", description="Base64 manifest")
connection_info_field = Field(alias="connectionInfo")


class QueryDTO(BaseModel):
    sql: str
    manifest_str: str = manifest_str_field
    column_dtypes: dict[str, str] | None = Field(
        alias="columnDtypes",
        description="If this field is set, it will forcibly convert the type.",
        default=None,
    )
    connection_info: ConnectionInfo = connection_info_field


class QueryBigQueryDTO(QueryDTO):
    connection_info: BigQueryConnectionInfo = connection_info_field


class QueryClickHouseDTO(QueryDTO):
    connection_info: ConnectionUrl | ClickHouseConnectionInfo = connection_info_field


class QueryMSSqlDTO(QueryDTO):
    connection_info: MSSqlConnectionInfo = connection_info_field


class QueryMySqlDTO(QueryDTO):
    connection_info: ConnectionUrl | MySqlConnectionInfo = connection_info_field


class QueryPostgresDTO(QueryDTO):
    connection_info: ConnectionUrl | PostgresConnectionInfo = connection_info_field


class QuerySnowflakeDTO(QueryDTO):
    connection_info: SnowflakeConnectionInfo = connection_info_field


class QueryTrinoDTO(QueryDTO):
    connection_info: ConnectionUrl | TrinoConnectionInfo = connection_info_field


class BigQueryConnectionInfo(BaseModel):
    project_id: str
    dataset_id: str
    credentials: str = Field(description="Base64 encode `credentials.json`")


class ClickHouseConnectionInfo(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str


class MSSqlConnectionInfo(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str
    driver: str = Field(
        default="FreeTDS",
        description="On Mac and Linux this is usually `FreeTDS. On Windows, it is usually `ODBC Driver 18 for SQL Server`",
    )


class MySqlConnectionInfo(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str


class ConnectionUrl(BaseModel):
    connection_url: str = Field(alias="connectionUrl")


class PostgresConnectionInfo(BaseModel):
    host: str = Field(examples=["localhost"])
    port: int = Field(examples=[5432])
    database: str
    user: str
    password: str


class SnowflakeConnectionInfo(BaseModel):
    user: str
    password: str
    account: str
    database: str
    sf_schema: str = Field(
        alias="schema"
    )  # Use `sf_schema` to avoid `schema` shadowing in BaseModel


class TrinoConnectionInfo(BaseModel):
    host: str
    port: int = Field(default=8080)
    catalog: str
    trino_schema: str = Field(
        alias="schema"
    )  # Use `trino_schema` to avoid `schema` shadowing in BaseModel
    user: str | None = None
    password: str | None = None


ConnectionInfo = (
    BigQueryConnectionInfo
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
