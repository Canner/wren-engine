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
