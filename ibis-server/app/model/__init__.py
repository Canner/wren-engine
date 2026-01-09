from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, SecretStr

from app.model.error import ErrorCode, WrenError

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
    connection_info: dict[str, Any] | ConnectionInfo = connection_info_field


class QueryBigQueryDTO(QueryDTO):
    connection_info: BigQueryConnectionUnion = connection_info_field


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
    connection_info: RedshiftConnectionUnion = connection_info_field


class QuerySnowflakeDTO(QueryDTO):
    connection_info: SnowflakeConnectionInfo = connection_info_field


class QueryDatabricksDTO(QueryDTO):
    connection_info: DatabricksConnectionUnion = connection_info_field


class QuerySparkDTO(QueryDTO):
    connection_info: SparkConnectionInfo = connection_info_field


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
    kwargs: dict[str, str] | None = Field(
        description="Additional keyword arguments to pass to the backend client connection.",
        default=None,
    )


class BigQueryConnectionInfo(BaseConnectionInfo):
    credentials: SecretStr = Field(
        description="Base64 encode `credentials.json`", examples=["eyJ..."]
    )
    job_timeout_ms: int | None = Field(
        description="Job timeout in milliseconds. If the job is not complete within the specified time, it will be cancelled.",
        default=None,
    )

    def get_billing_project_id(self) -> str | None:
        raise WrenError(
            ErrorCode.NOT_IMPLEMENTED,
            "get_billing_project_id not implemented by base class",
        )


class BigQueryDatasetConnectionInfo(BigQueryConnectionInfo):
    bigquery_type: Literal["dataset"] = "dataset"
    project_id: SecretStr = Field(description="GCP project id", examples=["my-project"])
    dataset_id: SecretStr = Field(
        description="BigQuery dataset id", examples=["my_dataset"]
    )

    def get_billing_project_id(self):
        return self.project_id.get_secret_value()

    def __hash__(self):
        return hash((self.project_id, self.dataset_id, self.credentials))


class BigQueryProjectConnectionInfo(BigQueryConnectionInfo):
    bigquery_type: Literal["project"] = "project"
    region: SecretStr = Field(
        description="the region of your BigQuery connection", examples=["US"]
    )
    billing_project_id: SecretStr = Field(
        description="the billing project id of your BigQuery connection",
        examples=["billing-project-1"],
    )

    def get_billing_project_id(self):
        return self.billing_project_id.get_secret_value()

    def __hash__(self):
        return hash(
            (
                self.region,
                self.billing_project_id,
                self.credentials,
            )
        )


BigQueryConnectionUnion = Annotated[
    Union[BigQueryDatasetConnectionInfo, BigQueryProjectConnectionInfo],
    Field(discriminator="bigquery_type", default="dataset"),
]


class AthenaConnectionInfo(BaseConnectionInfo):
    s3_staging_dir: SecretStr = Field(
        description="S3 staging directory for Athena queries",
        examples=["s3://my-bucket/athena-staging/"],
    )

    # ── Standard AWS credential chain (optional) ─────────────
    aws_access_key_id: SecretStr | None = Field(
        description="AWS access key ID. Optional if using IAM role, web identity token, or default credential chain.",
        examples=["AKIA..."],
        default=None,
    )
    aws_secret_access_key: SecretStr | None = Field(
        description="AWS secret access key. Optional if using IAM role, web identity token, or default credential chain.",
        examples=["my-secret-key"],
        default=None,
    )
    aws_session_token: SecretStr | None = Field(
        description="AWS session token (used for temporary credentials)",
        examples=["IQoJb3JpZ2luX2VjEJz//////////wEaCXVzLWVhc3QtMSJHMEUCIQD..."],
        default=None,
    )

    # ── Web identity federation (OIDC/JWT-based) ─────────────
    web_identity_token: SecretStr | None = Field(
        description=(
            "OIDC web identity token (JWT) used for AssumeRoleWithWebIdentity authentication. "
            "If provided, PyAthena will call STS to exchange it for temporary credentials."
        ),
        examples=["eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."],
        default=None,
    )
    role_arn: SecretStr | None = Field(
        description="The ARN of the role to assume with the web identity token.",
        examples=["arn:aws:iam::123456789012:role/YourAthenaRole"],
        default=None,
    )
    role_session_name: SecretStr | None = Field(
        description="The session name when assuming a role (optional).",
        examples=["PyAthena-session"],
        default=None,
    )

    # ── Regional and database settings ───────────────────────
    region_name: SecretStr = Field(
        description="AWS region for Athena. Optional; will use default region if not provided.",
        examples=["us-west-2", "us-east-1"],
        default=None,
    )
    schema_name: SecretStr | None = Field(
        alias="schema_name",
        description="The database name in Athena. Defaults to 'default'.",
        examples=["default"],
        default=SecretStr("default"),
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
    secure: bool = Field(
        description="Whether or not to use an authenticated endpoint",
        default=False,
        examples=[True, False],
    )
    settings: dict[str, str] | None = Field(
        description="Additional settings for ClickHouse connection",
        default=None,
        examples=[{"max_execution_time": "60"}],
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
        default=SecretStr("ENABLED"),
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
    redshift_type: Literal["redshift"] = "redshift"
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


# AWS Redshift IAM Connection Info
# This class is used to connect to AWS Redshift using IAM authentication.
class RedshiftIAMConnectionInfo(BaseConnectionInfo):
    redshift_type: Literal["redshift_iam"] = "redshift_iam"
    cluster_identifier: SecretStr = Field(
        description="the cluster identifier of your Redshift cluster",
        examples=["my-redshift-cluster"],
    )
    database: SecretStr = Field(
        description="the database name of your database", examples=["dev"]
    )
    user: SecretStr = Field(
        description="the username of your database", examples=["awsuser"]
    )
    region: SecretStr = Field(
        description="the region of your database", examples=["us-west-2"]
    )
    access_key_id: SecretStr = Field(
        description="the access key id of your database", examples=["AKIA..."]
    )
    access_key_secret: SecretStr = Field(
        description="the secret access key of your database", examples=["my-secret-key"]
    )


RedshiftConnectionUnion = Annotated[
    Union[RedshiftConnectionInfo, RedshiftIAMConnectionInfo],
    Field(discriminator="redshift_type"),
]


class SnowflakeConnectionInfo(BaseConnectionInfo):
    user: SecretStr = Field(
        description="the username of your database", examples=["admin"]
    )
    password: SecretStr | None = Field(
        description="the password of your database", examples=["password"], default=None
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
    warehouse: SecretStr | None = Field(
        description="the warehouse name of your database",
        examples=["COMPUTE_WH"],
        default=None,
    )
    private_key: SecretStr | None = Field(
        description="the private key for key pair authentication",
        examples=["private_key_content"],
        default=None,
    )
    kwargs: dict[str, str] | None = Field(
        description="Additional arguments passed to the DBAPI connection call.",
        default=None,
    )


class SparkConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(
        description="Spark Connect server hostname",
        examples=["localhost", "spark-connect.mycompany.internal"],
    )
    port: SecretStr = Field(description="the port of your spark connect server")


class DatabricksTokenConnectionInfo(BaseConnectionInfo):
    databricks_type: Literal["token"] = "token"
    server_hostname: SecretStr = Field(
        alias="serverHostname",
        description="the server hostname of your Databricks instance",
        examples=["dbc-xxxxxxxx-xxxx.cloud.databricks.com"],
    )
    http_path: SecretStr = Field(
        alias="httpPath",
        description="the HTTP path of your Databricks SQL warehouse",
        examples=["/sql/1.0/warehouses/xxxxxxxx"],
    )
    access_token: SecretStr = Field(
        alias="accessToken",
        description="the access token for your Databricks instance",
        examples=["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
    )


# https://docs.databricks.com/aws/en/dev-tools/python-sql-connector#oauth-machine-to-machine-m2m-authentication
class DatabricksServicePrincipalConnectionInfo(BaseConnectionInfo):
    databricks_type: Literal["service_principal"] = "service_principal"
    server_hostname: SecretStr = Field(
        alias="serverHostname",
        description="the server hostname of your Databricks instance",
        examples=["dbc-xxxxxxxx-xxxx.cloud.databricks.com"],
    )
    http_path: SecretStr = Field(
        alias="httpPath",
        description="the HTTP path of your Databricks SQL warehouse",
        examples=["/sql/1.0/warehouses/xxxxxxxx"],
    )
    client_id: SecretStr = Field(
        alias="clientId",
        description="the client ID for OAuth M2M authentication",
        examples=["your-client-id"],
    )
    client_secret: SecretStr = Field(
        alias="clientSecret",
        description="the client secret for OAuth M2M authentication",
        examples=["your-client-secret"],
    )
    azure_tenant_id: SecretStr | None = Field(
        alias="azureTenantId",
        description="the Azure tenant ID for OAuth M2M authentication",
        examples=["your-tenant-id"],
        default=None,
    )


DatabricksConnectionUnion = Annotated[
    Union[DatabricksTokenConnectionInfo, DatabricksServicePrincipalConnectionInfo],
    Field(discriminator="databricks_type"),
]


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
        description="File format",
        default="csv",
        examples=["csv", "parquet", "json", "duckdb"],
    )


class S3FileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(
        description="the root path of the s3 bucket", default="/", examples=["/data"]
    )
    format: str = Field(
        description="File format",
        default="csv",
        examples=["csv", "parquet", "json", "duckdb"],
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
        description="File format",
        default="csv",
        examples=["csv", "parquet", "json", "duckdb"],
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
        description="File format",
        default="csv",
        examples=["csv", "parquet", "json", "duckdb"],
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
    | BigQueryDatasetConnectionInfo
    | BigQueryProjectConnectionInfo
    | CannerConnectionInfo
    | ClickHouseConnectionInfo
    | ConnectionUrl
    | MSSqlConnectionInfo
    | MySqlConnectionInfo
    | OracleConnectionInfo
    | PostgresConnectionInfo
    | RedshiftConnectionInfo
    | RedshiftIAMConnectionInfo
    | SnowflakeConnectionInfo
    | SparkConnectionInfo
    | DatabricksTokenConnectionInfo
    | TrinoConnectionInfo
    | LocalFileConnectionInfo
    | S3FileConnectionInfo
    | MinioFileConnectionInfo
    | GcsFileConnectionInfo
)


class ValidateDTO(BaseModel):
    manifest_str: str = manifest_str_field
    parameters: dict
    connection_info: dict[str, Any] | ConnectionInfo = connection_info_field


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
    connection_info: dict[str, Any] | ConnectionInfo = connection_info_field
    sql: str


class ConfigModel(BaseModel):
    diagnose: bool


class SSLMode(str, Enum):
    DISABLED = "disabled"
    ENABLED = "enabled"
    VERIFY_CA = "verify_ca"
