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


class BaseConnectionInfo(BaseModel):
    def to_key_string(self) -> str:
        key_parts = []

        # Get all fields that are SecretStr and get their values
        for _, field_value in self:
            if isinstance(field_value, SecretStr):
                key_parts.append(field_value.get_secret_value())

        return "|".join(key_parts)


class QueryDTO(BaseModel):
    sql: str
    manifest_str: str = manifest_str_field
    connection_info: ConnectionInfo = connection_info_field


class QueryBigQueryDTO(QueryDTO):
    connection_info: BigQueryConnectionInfo = connection_info_field


class QueryAthenaDTO(QueryDTO):
    connection_info: AthenaConnectionInfo = connection_info_field


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


class QueryRedshiftDTO(QueryDTO):
    connection_info: RedshiftConnectionInfo = connection_info_field


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


class ConnectionUrl(BaseConnectionInfo):
    connection_url: SecretStr = Field(alias="connectionUrl")


class BigQueryConnectionInfo(BaseConnectionInfo):
    project_id: SecretStr = Field(description="GCP project id", examples=["my-project"])
    dataset_id: SecretStr = Field(
        description="BigQuery dataset id", examples=["my_dataset"]
    )
    credentials: SecretStr = Field(
        description="Base64 encode `credentials.json`", examples=["eyJ..."]
    )


class AthenaConnectionInfo(BaseConnectionInfo):
    s3_staging_dir: SecretStr = Field(
        description="S3 staging directory for Athena queries",
        examples=["s3://my-bucket/athena-staging/"],
    )
    aws_access_key_id: SecretStr = Field(
        description="AWS access key ID", examples=["AKIA..."]
    )
    aws_secret_access_key: SecretStr = Field(
        description="AWS secret access key", examples=["my-secret-key"]
    )
    region_name: SecretStr = Field(
        description="AWS region for Athena", examples=["us-west-2", "us-east-1"]
    )
    schema_name: SecretStr = Field(
        alias="schema_name",
        description="The database name in Athena",
        examples=["default"],
    )


class CannerConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        description="the hostname of your database", examples=["localhost"]
    )
    port: SecretStr = Field(description="the port of your database", examples=["8080"])
    user: SecretStr = Field(
        description="the username of your database", examples=["admin"]
    )
    pat: SecretStr = Field(
        description="the personal access token of your database", examples=["eyJ..."]
    )
    workspace: SecretStr = Field(
        description="the workspace of your database", examples=["default"]
    )
    enable_ssl: bool = Field(
        description="Enable SSL connection", default=False, alias="enableSSL"
    )


class ClickHouseConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        description="the hostname of your database", examples=["localhost"]
    )
    port: SecretStr = Field(description="the port of your database", examples=["8123"])
    database: SecretStr = Field(
        description="the database name of your database", examples=["default"]
    )
    user: SecretStr = Field(
        description="the username of your database", examples=["default"]
    )
    password: SecretStr | None = Field(
        description="the password of your database", examples=["password"], default=None
    )
    kwargs: dict[str, str] | None = Field(
        description="Client specific keyword arguments", default=None
    )


class MSSqlConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        description="the hostname of your database", examples=["localhost"]
    )
    port: SecretStr = Field(description="the port of your database", examples=["1433"])
    database: SecretStr = Field(
        description="the database name of your database", examples=["master"]
    )
    user: SecretStr = Field(
        description="the username of your database", examples=["sa"]
    )
    password: SecretStr | None = Field(
        description="the password of your database", examples=["password"], default=None
    )
    driver: str = Field(
        default="ODBC Driver 18 for SQL Server", description="The ODBC driver to use"
    )
    tds_version: str = Field(
        default="8.0", alias="TDS_Version", description="The TDS version to use"
    )
    kwargs: dict[str, str] | None = Field(
        description="Additional keyword arguments to pass to PyODBC", default=None
    )


class MySqlConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        description="the hostname of your database", examples=["localhost"]
    )
    port: SecretStr = Field(description="the port of your database", examples=["3306"])
    database: SecretStr = Field(
        description="the database name of your database", examples=["default"]
    )
    user: SecretStr = Field(
        description="the username of your database", examples=["root"]
    )
    password: SecretStr | None = Field(
        description="the password of your database", examples=["password"], default=None
    )
    ssl_mode: SecretStr | None = Field(
        alias="sslMode",
        default="ENABLED",
        description="Use ssl connection or not. The default value is `ENABLED` because MySQL uses `caching_sha2_password` by default and the driver MySQLdb support caching_sha2_password with ssl only.",
        examples=["DISABLED", "ENABLED", "VERIFY_CA"],
    )
    ssl_ca: SecretStr | None = Field(
        alias="sslCA", default=None, description="The path to the CA certificate file"
    )
    kwargs: dict[str, str] | None = Field(
        description="Additional keyword arguments to pass to PyMySQL", default=None
    )


class PostgresConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        examples=["localhost"], description="the hostname of your database"
    )
    port: SecretStr = Field(examples=[5432], description="the port of your database")
    database: SecretStr = Field(
        examples=["postgres"], description="the database name of your database"
    )
    user: SecretStr = Field(
        examples=["postgres"], description="the username of your database"
    )
    password: SecretStr | None = Field(
        examples=["password"], description="the password of your database", default=None
    )
    kwargs: dict[str, str] | None = Field(
        description="Additional keyword arguments to pass to the backend client connection.",
        default=None,
    )


class OracleConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        examples=["localhost"],
        description="the hostname of your database",
        default="localhost",
    )
    port: SecretStr = Field(
        examples=[1521], description="the port of your database", default="1521"
    )
    database: SecretStr = Field(
        examples=["orcl"],
        description="the database name of your database",
        default="orcl",
    )
    user: SecretStr = Field(
        examples=["admin"], description="the username of your database"
    )
    password: SecretStr | None = Field(
        examples=["password"], description="the password of your database", default=None
    )
    dsn: SecretStr | None = Field(
        default=None,
        description="An Oracle Data Source Name. If provided, overrides all other connection arguments except username and password.",
        examples=["localhost:1521/orcl"],
    )


class RedshiftConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        description="the hostname of your database", examples=["localhost"]
    )
    port: SecretStr = Field(description="the port of your database", examples=["5439"])
    database: SecretStr = Field(
        description="the database name of your database", examples=["dev"]
    )
    user: SecretStr = Field(
        description="the username of your database", examples=["awsuser"]
    )
    password: SecretStr = Field(
        description="the password of your database", examples=["password"]
    )


class SnowflakeConnectionInfo(BaseConnectionInfo):
    user: SecretStr = Field(
        description="the username of your database", examples=["admin"]
    )
    password: SecretStr = Field(
        description="the password of your database", examples=["password"]
    )
    account: SecretStr = Field(
        description="the account name of your database", examples=["myaccount"]
    )
    database: SecretStr = Field(
        description="the database name of your database", examples=["mydb"]
    )
    sf_schema: SecretStr = Field(
        alias="schema",
        description="the schema name of your database",
        examples=["myschema"],
    )  # Use `sf_schema` to avoid `schema` shadowing in BaseModel
    kwargs: dict[str, str] | None = Field(
        description="Additional arguments passed to the DBAPI connection call.",
        default=None,
    )


class TrinoConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        description="the hostname of your database", examples=["localhost"]
    )
    port: SecretStr = Field(default="8080", description="the port of your database")
    catalog: SecretStr = Field(
        description="the catalog name of your database", examples=["hive"]
    )
    trino_schema: SecretStr = Field(
        alias="schema",
        description="the schema name of your database",
        examples=["default"],
    )  # Use `trino_schema` to avoid `schema` shadowing in BaseModel
    user: SecretStr | None = Field(
        description="the username of your database", examples=["admin"], default=None
    )
    password: SecretStr | None = Field(
        description="the password of your database", examples=["password"], default=None
    )
    kwargs: dict[str, str] | None = Field(
        description="Additional keyword arguments passed directly to the trino.dbapi.connect API.",
        default=None,
    )


class LocalFileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(
        description="the root path of the local file", default="/", examples=["/data"]
    )
    format: str = Field(
        description="File format", default="csv", examples=["csv", "parquet", "json"]
    )


class S3FileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(
        description="the root path of the s3 bucket", default="/", examples=["/data"]
    )
    format: str = Field(
        description="File format", default="csv", examples=["csv", "parquet", "json"]
    )
    bucket: SecretStr = Field(
        description="the name of the s3 bucket", examples=["my-bucket"]
    )
    region: SecretStr = Field(
        description="the region of the s3 bucket", examples=["us-west-2"]
    )
    access_key: SecretStr = Field(
        description="the access key of the s3 bucket", examples=["my-access-key"]
    )
    secret_key: SecretStr = Field(
        description="the secret key of the s3 bucket", examples=["my-secret-key"]
    )


class MinioFileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(
        description="the root path of the minio bucket", default="/", examples=["/data"]
    )
    format: str = Field(
        description="File format", default="csv", examples=["csv", "parquet", "json"]
    )
    ssl_enabled: bool = Field(
        description="use the ssl connection or not",
        default=False,
        examples=[True, False],
    )
    endpoint: SecretStr = Field(
        description="the endpoint of the minio bucket", examples=["localhost:9000"]
    )
    bucket: SecretStr = Field(
        description="the name of the minio bucket", examples=["my-bucket"]
    )
    access_key: SecretStr = Field(
        description="the account of the minio bucket", examples=["my-account"]
    )
    secret_key: SecretStr = Field(
        description="the The password of the minio bucket", examples=["my-password"]
    )


class GcsFileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(
        description="the root path of the gcs bucket", default="/", examples=["/data"]
    )
    format: str = Field(
        description="File format", default="csv", examples=["csv", "parquet", "json"]
    )
    bucket: SecretStr = Field(
        description="the name of the gcs bucket", examples=["my-bucket"]
    )
    key_id: SecretStr = Field(
        description="the key id of the gcs bucket", examples=["my-key-id"]
    )
    secret_key: SecretStr = Field(
        description="the secret key of the gcs bucket", examples=["my-secret-key"]
    )
    credentials: SecretStr = Field(
        description="Base64 encode `credentials.json`",
        examples=["eyJ..."],
        default=None,
    )


ConnectionInfo = (
    AthenaConnectionInfo
    | BigQueryConnectionInfo
    | CannerConnectionInfo
    | ConnectionUrl
    | MSSqlConnectionInfo
    | MySqlConnectionInfo
    | OracleConnectionInfo
    | PostgresConnectionInfo
    | RedshiftConnectionInfo
    | SnowflakeConnectionInfo
    | TrinoConnectionInfo
    | LocalFileConnectionInfo
    | S3FileConnectionInfo
    | MinioFileConnectionInfo
    | GcsFileConnectionInfo
)


class ValidateDTO(BaseModel):
    manifest_str: str = manifest_str_field
    parameters: dict
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
