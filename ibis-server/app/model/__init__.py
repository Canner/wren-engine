from __future__ import annotations

from abc import ABC
from enum import Enum

from pydantic import BaseModel, Field, SecretStr
from starlette.status import (
    HTTP_404_NOT_FOUND,
    HTTP_422_UNPROCESSABLE_ENTITY,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

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


class QueryOracleDTO(QueryDTO):
    connection_info: ConnectionUrl | OracleConnectionInfo = connection_info_field


class QueryPostgresDTO(QueryDTO):
    connection_info: ConnectionUrl | PostgresConnectionInfo = connection_info_field


class QuerySnowflakeDTO(QueryDTO):
    connection_info: SnowflakeConnectionInfo = connection_info_field


class QueryTrinoDTO(QueryDTO):
    connection_info: ConnectionUrl | TrinoConnectionInfo = connection_info_field


class QueryLocalFileDTO(QueryDTO):
    connection_info: LocalFileConnectionInfo = connection_info_field


class QueryS3FileDTO(QueryDTO):
    connection_info: S3FileConnectionInfo = connection_info_field


class QueryMinioFileDTO(QueryDTO):
    connection_info: MinioFileConnectionInfo = connection_info_field


class QueryGcsFileDTO(QueryDTO):
    connection_info: GcsFileConnectionInfo = connection_info_field


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
    enable_ssl: bool = Field(
        description="Enable SSL connection", default=False, alias="enableSSL"
    )


class ClickHouseConnectionInfo(BaseModel):
    host: SecretStr
    port: SecretStr
    database: SecretStr
    user: SecretStr
    password: SecretStr | None = None


class MSSqlConnectionInfo(BaseModel):
    host: SecretStr
    port: SecretStr
    database: SecretStr
    user: SecretStr
    password: SecretStr | None = None
    driver: str = Field(default="ODBC Driver 18 for SQL Server")
    tds_version: str = Field(default="8.0", alias="TDS_Version")
    kwargs: dict[str, str] | None = Field(
        description="Additional keyword arguments to pass to PyODBC", default=None
    )


class MySqlConnectionInfo(BaseModel):
    host: SecretStr
    port: SecretStr
    database: SecretStr
    user: SecretStr
    password: SecretStr | None = None
    ssl_mode: SecretStr | None = Field(
        alias="sslMode",
        default="ENABLED",
        description="Use ssl connection or not. The default value is `ENABLED` because MySQL uses `caching_sha2_password` by default and the driver MySQLdb support caching_sha2_password with ssl only.",
    )
    ssl_ca: SecretStr | None = Field(alias="sslCA", default=None)
    kwargs: dict[str, str] | None = Field(
        description="Additional keyword arguments to pass to PyMySQL", default=None
    )


class ConnectionUrl(BaseModel):
    connection_url: SecretStr = Field(alias="connectionUrl")


class PostgresConnectionInfo(BaseModel):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretStr = Field(examples=[5432])
    database: SecretStr
    user: SecretStr
    password: SecretStr | None = None


class OracleConnectionInfo(BaseModel):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretStr = Field(examples=[1521])
    database: SecretStr
    user: SecretStr
    password: SecretStr | None = None


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


class LocalFileConnectionInfo(BaseModel):
    url: SecretStr
    format: str = Field(
        description="File format", default="csv", examples=["csv", "parquet", "json"]
    )


class S3FileConnectionInfo(BaseModel):
    url: SecretStr = Field(description="the root path of the s3 bucket", default="/")
    format: str = Field(
        description="File format", default="csv", examples=["csv", "parquet", "json"]
    )
    bucket: SecretStr
    region: SecretStr
    access_key: SecretStr
    secret_key: SecretStr


class MinioFileConnectionInfo(BaseModel):
    url: SecretStr = Field(description="the root path of the minio bucket", default="/")
    format: str = Field(
        description="File format", default="csv", examples=["csv", "parquet", "json"]
    )
    ssl_enabled: bool = Field(
        description="use the ssl connection or not", default=False
    )
    endpoint: SecretStr
    bucket: SecretStr
    access_key: SecretStr
    secret_key: SecretStr


class GcsFileConnectionInfo(BaseModel):
    url: SecretStr = Field(description="the root path of the gcs bucket", default="/")
    format: str = Field(
        description="File format", default="csv", examples=["csv", "parquet", "json"]
    )
    bucket: SecretStr
    key_id: SecretStr
    secret_key: SecretStr
    credentials: SecretStr = Field(description="Base64 encode `credentials.json`")


ConnectionInfo = (
    BigQueryConnectionInfo
    | CannerConnectionInfo
    | ConnectionUrl
    | MSSqlConnectionInfo
    | MySqlConnectionInfo
    | OracleConnectionInfo
    | PostgresConnectionInfo
    | SnowflakeConnectionInfo
    | TrinoConnectionInfo
    | LocalFileConnectionInfo
    | S3FileConnectionInfo
    | MinioFileConnectionInfo
    | GcsFileConnectionInfo
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


class TranspileDTO(BaseModel):
    manifest_str: str = manifest_str_field
    connection_info: ConnectionInfo = connection_info_field
    sql: str


class ConfigModel(BaseModel):
    diagnose: bool


class UnknownIbisError(Exception):
    def __init__(self, message):
        self.message = f"Unknown ibis error: {message!s}"


class CustomHttpError(ABC, Exception):
    status_code: int


class InternalServerError(CustomHttpError):
    status_code = HTTP_500_INTERNAL_SERVER_ERROR


class UnprocessableEntityError(CustomHttpError):
    status_code = HTTP_422_UNPROCESSABLE_ENTITY


class NotFoundError(CustomHttpError):
    status_code = HTTP_404_NOT_FOUND


class SSLMode(str, Enum):
    DISABLED = "disabled"
    ENABLED = "enabled"
    VERIFY_CA = "verify_ca"
