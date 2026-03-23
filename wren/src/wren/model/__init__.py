"""Connection info models and DTOs for the wren package."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, BeforeValidator, Field, SecretStr

from wren.model.error import ErrorCode, WrenError

SecretPort = Annotated[
    SecretStr, BeforeValidator(lambda v: str(v) if isinstance(v, int) else v)
]


class BaseConnectionInfo(BaseModel):
    model_config = {"populate_by_name": True}

    def to_key_string(self) -> str:
        key_parts = []
        for _, field_value in self:
            if isinstance(field_value, SecretStr):
                key_parts.append(field_value.get_secret_value())
        return "|".join(key_parts)


class BigQueryConnectionInfo(BaseConnectionInfo):
    credentials: SecretStr = Field(
        description="Base64 encode `credentials.json`", examples=["eyJ..."]
    )
    job_timeout_ms: int | None = Field(default=None)

    def get_billing_project_id(self) -> str | None:
        raise WrenError(
            ErrorCode.NOT_IMPLEMENTED,
            "get_billing_project_id not implemented by base class",
        )


class BigQueryDatasetConnectionInfo(BigQueryConnectionInfo):
    bigquery_type: Literal["dataset"] = "dataset"
    project_id: SecretStr = Field(examples=["my-project"])
    dataset_id: SecretStr = Field(examples=["my_dataset"])

    def get_billing_project_id(self):
        return self.project_id.get_secret_value()

    def __hash__(self):
        return hash((self.project_id, self.dataset_id, self.credentials))


class BigQueryProjectConnectionInfo(BigQueryConnectionInfo):
    bigquery_type: Literal["project"] = "project"
    region: SecretStr = Field(examples=["US"])
    billing_project_id: SecretStr = Field(examples=["billing-project-1"])

    def get_billing_project_id(self):
        return self.billing_project_id.get_secret_value()

    def __hash__(self):
        return hash((self.region, self.billing_project_id, self.credentials))


BigQueryConnectionUnion = Annotated[
    Union[BigQueryDatasetConnectionInfo, BigQueryProjectConnectionInfo],
    Field(discriminator="bigquery_type", default="dataset"),
]


class AthenaConnectionInfo(BaseConnectionInfo):
    s3_staging_dir: SecretStr = Field(examples=["s3://my-bucket/athena-staging/"])
    aws_access_key_id: SecretStr | None = Field(default=None)
    aws_secret_access_key: SecretStr | None = Field(default=None)
    aws_session_token: SecretStr | None = Field(default=None)
    web_identity_token: SecretStr | None = Field(default=None)
    role_arn: SecretStr | None = Field(default=None)
    role_session_name: SecretStr | None = Field(default=None)
    region_name: SecretStr = Field(examples=["us-west-2"], default=None)
    schema_name: SecretStr | None = Field(
        alias="schema_name", default=SecretStr("default")
    )


class CannerConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(examples=["8080"])
    user: SecretStr = Field(examples=["admin"])
    pat: SecretStr = Field(examples=["eyJ..."])
    workspace: SecretStr = Field(examples=["default"])
    enable_ssl: bool = Field(default=False, alias="enableSSL")


class ClickHouseConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(examples=["8123"])
    database: SecretStr = Field(examples=["default"])
    user: SecretStr = Field(examples=["default"])
    password: SecretStr | None = Field(default=None)
    secure: bool = Field(default=False)
    settings: dict[str, str] | None = Field(default=None)
    kwargs: dict[str, str] | None = Field(default=None)


class MSSqlConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(examples=["1433"])
    database: SecretStr = Field(examples=["master"])
    user: SecretStr = Field(examples=["sa"])
    password: SecretStr | None = Field(default=None)
    driver: str = Field(default="ODBC Driver 18 for SQL Server")
    tds_version: str = Field(default="8.0", alias="TDS_Version")
    kwargs: dict[str, str] | None = Field(default=None)


class MySqlConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(examples=["3306"])
    database: SecretStr = Field(examples=["default"])
    user: SecretStr = Field(examples=["root"])
    password: SecretStr | None = Field(default=None)
    ssl_mode: SecretStr | None = Field(alias="sslMode", default=SecretStr("ENABLED"))
    ssl_ca: SecretStr | None = Field(alias="sslCA", default=None)
    kwargs: dict[str, str] | None = Field(default=None)


class DorisConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(examples=["9030"])
    database: SecretStr = Field(examples=["default"])
    user: SecretStr = Field(examples=["root"])
    password: SecretStr | None = Field(default=None)
    kwargs: dict[str, str] | None = Field(default=None)


class PostgresConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(examples=["5432"])
    database: SecretStr = Field(examples=["postgres"])
    user: SecretStr = Field(examples=["postgres"])
    password: SecretStr | None = Field(default=None)
    kwargs: dict[str, str] | None = Field(default=None)


class OracleConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(default="localhost", examples=["localhost"])
    port: SecretPort = Field(default="1521", examples=[1521])
    database: SecretStr = Field(default="orcl", examples=["orcl"])
    user: SecretStr = Field(examples=["admin"])
    password: SecretStr | None = Field(default=None)
    dsn: SecretStr | None = Field(default=None)


class RedshiftConnectionInfo(BaseConnectionInfo):
    redshift_type: Literal["redshift"] = "redshift"
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(examples=["5439"])
    database: SecretStr = Field(examples=["dev"])
    user: SecretStr = Field(examples=["awsuser"])
    password: SecretStr = Field(examples=["password"])


class RedshiftIAMConnectionInfo(BaseConnectionInfo):
    redshift_type: Literal["redshift_iam"] = "redshift_iam"
    cluster_identifier: SecretStr = Field(examples=["my-redshift-cluster"])
    database: SecretStr = Field(examples=["dev"])
    user: SecretStr = Field(examples=["awsuser"])
    region: SecretStr = Field(examples=["us-west-2"])
    access_key_id: SecretStr = Field(examples=["AKIA..."])
    access_key_secret: SecretStr = Field(examples=["my-secret-key"])


RedshiftConnectionUnion = Annotated[
    Union[RedshiftConnectionInfo, RedshiftIAMConnectionInfo],
    Field(discriminator="redshift_type"),
]


class SnowflakeConnectionInfo(BaseConnectionInfo):
    user: SecretStr = Field(examples=["admin"])
    password: SecretStr | None = Field(default=None)
    account: SecretStr = Field(examples=["myaccount"])
    database: SecretStr = Field(examples=["mydb"])
    sf_schema: SecretStr = Field(alias="schema", examples=["myschema"])
    warehouse: SecretStr | None = Field(default=None)
    private_key: SecretStr | None = Field(default=None)
    kwargs: dict[str, str] | None = Field(default=None)


class SparkConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(examples=["15002"])


class DatabricksTokenConnectionInfo(BaseConnectionInfo):
    databricks_type: Literal["token"] = "token"
    server_hostname: SecretStr = Field(
        alias="serverHostname", examples=["dbc-xxx.cloud.databricks.com"]
    )
    http_path: SecretStr = Field(alias="httpPath", examples=["/sql/1.0/warehouses/xxx"])
    access_token: SecretStr = Field(alias="accessToken", examples=["dapi..."])


class DatabricksServicePrincipalConnectionInfo(BaseConnectionInfo):
    databricks_type: Literal["service_principal"] = "service_principal"
    server_hostname: SecretStr = Field(alias="serverHostname")
    http_path: SecretStr = Field(alias="httpPath")
    client_id: SecretStr = Field(alias="clientId")
    client_secret: SecretStr = Field(alias="clientSecret")
    azure_tenant_id: SecretStr | None = Field(alias="azureTenantId", default=None)


DatabricksConnectionUnion = Annotated[
    Union[DatabricksTokenConnectionInfo, DatabricksServicePrincipalConnectionInfo],
    Field(discriminator="databricks_type"),
]


class TrinoConnectionInfo(BaseConnectionInfo):
    host: SecretStr = Field(examples=["localhost"])
    port: SecretPort = Field(default="8080")
    catalog: SecretStr = Field(examples=["hive"])
    trino_schema: SecretStr = Field(alias="schema", examples=["default"])
    user: SecretStr | None = Field(default=None)
    password: SecretStr | None = Field(default=None)
    kwargs: dict[str, str] | None = Field(default=None)


class LocalFileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(default="/", examples=["/data"])
    format: str = Field(default="csv", examples=["csv", "parquet", "json", "duckdb"])


class S3FileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(default="/", examples=["/data"])
    format: str = Field(default="csv")
    bucket: SecretStr = Field(examples=["my-bucket"])
    region: SecretStr = Field(examples=["us-west-2"])
    access_key: SecretStr = Field(examples=["my-access-key"])
    secret_key: SecretStr = Field(examples=["my-secret-key"])


class MinioFileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(default="/", examples=["/data"])
    format: str = Field(default="csv")
    ssl_enabled: bool = Field(default=False)
    endpoint: SecretStr = Field(examples=["localhost:9000"])
    bucket: SecretStr = Field(examples=["my-bucket"])
    access_key: SecretStr = Field(examples=["my-account"])
    secret_key: SecretStr = Field(examples=["my-password"])


class GcsFileConnectionInfo(BaseConnectionInfo):
    url: SecretStr = Field(default="/", examples=["/data"])
    format: str = Field(default="csv")
    bucket: SecretStr = Field(examples=["my-bucket"])
    key_id: SecretStr = Field(examples=["my-key-id"])
    secret_key: SecretStr = Field(examples=["my-secret-key"])
    credentials: SecretStr | None = Field(default=None, examples=["eyJ..."])


class ConnectionUrl(BaseConnectionInfo):
    connection_url: SecretStr = Field(alias="connectionUrl")
    kwargs: dict[str, str] | None = Field(default=None)


ConnectionInfo = (
    AthenaConnectionInfo
    | BigQueryDatasetConnectionInfo
    | BigQueryProjectConnectionInfo
    | CannerConnectionInfo
    | ClickHouseConnectionInfo
    | ConnectionUrl
    | MSSqlConnectionInfo
    | MySqlConnectionInfo
    | DorisConnectionInfo
    | OracleConnectionInfo
    | PostgresConnectionInfo
    | RedshiftConnectionInfo
    | RedshiftIAMConnectionInfo
    | SnowflakeConnectionInfo
    | SparkConnectionInfo
    | DatabricksTokenConnectionInfo
    | DatabricksServicePrincipalConnectionInfo
    | TrinoConnectionInfo
    | LocalFileConnectionInfo
    | S3FileConnectionInfo
    | MinioFileConnectionInfo
    | GcsFileConnectionInfo
)


class SSLMode(str, Enum):
    DISABLED = "disabled"
    ENABLED = "enabled"
    VERIFY_CA = "verify_ca"
