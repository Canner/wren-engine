from __future__ import annotations

import base64
import ssl
import urllib
from enum import Enum, StrEnum, auto
from json import loads
from typing import Any
from urllib.parse import unquote_plus

import ibis
from google.oauth2 import service_account
from ibis import BaseBackend

from app.model import (
    AthenaConnectionInfo,
    BigQueryConnectionInfo,
    CannerConnectionInfo,
    ClickHouseConnectionInfo,
    ConnectionInfo,
    ConnectionUrl,
    GcsFileConnectionInfo,
    LocalFileConnectionInfo,
    MinioFileConnectionInfo,
    MSSqlConnectionInfo,
    MySqlConnectionInfo,
    OracleConnectionInfo,
    PostgresConnectionInfo,
    QueryAthenaDTO,
    QueryBigQueryDTO,
    QueryCannerDTO,
    QueryClickHouseDTO,
    QueryDTO,
    QueryGcsFileDTO,
    QueryLocalFileDTO,
    QueryMinioFileDTO,
    QueryMSSqlDTO,
    QueryMySqlDTO,
    QueryOracleDTO,
    QueryPostgresDTO,
    QueryRedshiftDTO,
    QueryS3FileDTO,
    QuerySnowflakeDTO,
    QueryTrinoDTO,
    RedshiftConnectionInfo,
    RedshiftIAMConnectionInfo,
    S3FileConnectionInfo,
    SnowflakeConnectionInfo,
    SSLMode,
    TrinoConnectionInfo,
)

X_WREN_DB_STATEMENT_TIMEOUT = "x-wren-db-statement_timeout"


class DataSource(StrEnum):
    athena = auto()
    bigquery = auto()
    canner = auto()
    clickhouse = auto()
    mssql = auto()
    mysql = auto()
    oracle = auto()
    postgres = auto()
    redshift = auto()
    snowflake = auto()
    trino = auto()
    local_file = auto()
    s3_file = auto()
    minio_file = auto()
    gcs_file = auto()

    def get_connection(self, info: ConnectionInfo) -> BaseBackend:
        try:
            return DataSourceExtension[self].get_connection(info)
        except KeyError:
            raise NotImplementedError(f"Unsupported data source: {self}")

    def get_dto_type(self):
        try:
            return DataSourceExtension[self].dto
        except KeyError:
            raise NotImplementedError(f"Unsupported data source: {self}")

    def get_connection_info(
        self, data: dict[str, Any] | ConnectionInfo, headers: dict[str, str]
    ) -> ConnectionInfo:
        """Build a ConnectionInfo object from the provided data and add requried configuration from headers."""
        if isinstance(data, ConnectionInfo):
            info = data
        else:
            info = self._build_connection_info(data)
        match self:
            case DataSource.postgres:
                kwargs = info.kwargs if info.kwargs else dict()
                if not hasattr(info, "connect_timeout"):
                    kwargs["connect_timeout"] = 120

                options = kwargs.get("options", "")
                if "statement_timeout" not in options:
                    if options:
                        options += " "
                    options += f"-c statement_timeout={headers.get(X_WREN_DB_STATEMENT_TIMEOUT, 180)}s"
                    kwargs["options"] = options
                info.kwargs = kwargs
            case DataSource.clickhouse:
                session_timeout = headers.get(X_WREN_DB_STATEMENT_TIMEOUT, 180)
                if info.settings is None:
                    info.settings = {}
                if "max_execution_time" not in info.settings:
                    info.settings["max_execution_time"] = int(session_timeout)
            case DataSource.trino:
                session_timeout = headers.get(X_WREN_DB_STATEMENT_TIMEOUT, 180)
                if info.kwargs is None:
                    info.kwargs = {}
                session_properties = info.kwargs.get("session_properties", {})
                if "query_max_execution_time" not in session_properties:
                    session_properties["query_max_execution_time"] = (
                        f"{session_timeout}s"
                    )
                info.kwargs["session_properties"] = session_properties
        return info

    def _build_connection_info(self, data: dict) -> ConnectionInfo:
        """Build a ConnectionInfo object from the provided data."""
        # Check if data contains connectionUrl for connection string-based connections
        if "connectionUrl" in data or "connection_url" in data:
            if self == DataSource.clickhouse:
                return self._handle_clickhouse_url(
                    urllib.parse.urlparse(
                        data.get("connectionUrl", data.get("connection_url"))
                    )
                )
            return ConnectionUrl.model_validate(data)

        match self:
            case DataSource.athena:
                return AthenaConnectionInfo.model_validate(data)
            case DataSource.bigquery:
                return BigQueryConnectionInfo.model_validate(data)
            case DataSource.canner:
                return CannerConnectionInfo.model_validate(data)
            case DataSource.clickhouse:
                return ClickHouseConnectionInfo.model_validate(data)
            case DataSource.mssql:
                return MSSqlConnectionInfo.model_validate(data)
            case DataSource.mysql:
                return MySqlConnectionInfo.model_validate(data)
            case DataSource.oracle:
                return OracleConnectionInfo.model_validate(data)
            case DataSource.postgres:
                return PostgresConnectionInfo.model_validate(data)
            case DataSource.redshift:
                if "redshift_type" in data and data["redshift_type"] == "redshift_iam":
                    return RedshiftIAMConnectionInfo.model_validate(data)
                return RedshiftConnectionInfo.model_validate(data)
            case DataSource.snowflake:
                return SnowflakeConnectionInfo.model_validate(data)
            case DataSource.trino:
                return TrinoConnectionInfo.model_validate(data)
            case DataSource.local_file:
                return LocalFileConnectionInfo.model_validate(data)
            case DataSource.s3_file:
                return S3FileConnectionInfo.model_validate(data)
            case DataSource.minio_file:
                return MinioFileConnectionInfo.model_validate(data)
            case DataSource.gcs_file:
                return GcsFileConnectionInfo.model_validate(data)
            case _:
                raise NotImplementedError(f"Unsupported data source: {self}")

    def _handle_clickhouse_url(
        self, parsed: urllib.parse.ParseResult
    ) -> ClickHouseConnectionInfo:
        if not parsed.scheme or parsed.scheme != "clickhouse":
            raise ValueError("Invalid connection URL for ClickHouse")
        kwargs = {}
        if parsed.username:
            kwargs["user"] = parsed.username
        if parsed.password:
            kwargs["password"] = unquote_plus(parsed.password)
        if parsed.hostname:
            kwargs["host"] = parsed.hostname
        if parsed.port:
            kwargs["port"] = str(parsed.port)
        if database := parsed.path[1:]:
            kwargs["database"] = database
        parsed_kwargs = dict(urllib.parse.parse_qsl(parsed.query))
        if "secure" in parsed_kwargs:
            kwargs["secure"] = self._safe_strtobool(parsed_kwargs["secure"])
            parsed_kwargs.pop("secure")
        kwargs["kwargs"] = parsed_kwargs
        return ClickHouseConnectionInfo(**kwargs)

    def _safe_strtobool(self, val: str) -> bool:
        return val.lower() in {"1", "true", "yes", "y"}


class DataSourceExtension(Enum):
    athena = QueryAthenaDTO
    bigquery = QueryBigQueryDTO
    canner = QueryCannerDTO
    clickhouse = QueryClickHouseDTO
    mssql = QueryMSSqlDTO
    mysql = QueryMySqlDTO
    oracle = QueryOracleDTO
    postgres = QueryPostgresDTO
    redshift = QueryRedshiftDTO
    snowflake = QuerySnowflakeDTO
    trino = QueryTrinoDTO
    local_file = QueryLocalFileDTO
    s3_file = QueryS3FileDTO
    minio_file = QueryMinioFileDTO
    gcs_file = QueryGcsFileDTO

    def __init__(self, dto: QueryDTO):
        self.dto = dto

    def get_connection(self, info: ConnectionInfo) -> BaseBackend:
        try:
            if hasattr(info, "connection_url"):
                kwargs = info.kwargs if info.kwargs else {}
                return ibis.connect(info.connection_url.get_secret_value(), **kwargs)
            if self.name in {"local_file", "redshift"}:
                raise NotImplementedError(
                    f"{self.name} connection is not implemented to get ibis backend"
                )
            return getattr(self, f"get_{self.name}_connection")(info)
        except KeyError:
            raise NotImplementedError(f"Unsupported data source: {self}")

    @staticmethod
    def get_athena_connection(info: AthenaConnectionInfo) -> BaseBackend:
        return ibis.athena.connect(
            s3_staging_dir=info.s3_staging_dir.get_secret_value(),
            aws_access_key_id=info.aws_access_key_id.get_secret_value(),
            aws_secret_access_key=info.aws_secret_access_key.get_secret_value(),
            region_name=info.region_name.get_secret_value(),
            schema_name=info.schema_name.get_secret_value(),
        )

    @staticmethod
    def get_bigquery_connection(info: BigQueryConnectionInfo) -> BaseBackend:
        credits_json = loads(
            base64.b64decode(info.credentials.get_secret_value()).decode("utf-8")
        )
        credentials = service_account.Credentials.from_service_account_info(
            credits_json
        )
        credentials = credentials.with_scopes(
            [
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/cloud-platform",
            ]
        )
        return ibis.bigquery.connect(
            project_id=info.project_id.get_secret_value(),
            dataset_id=info.dataset_id.get_secret_value(),
            credentials=credentials,
        )

    @staticmethod
    def get_canner_connection(info: CannerConnectionInfo) -> BaseBackend:
        return ibis.postgres.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.workspace.get_secret_value(),
            user=info.user.get_secret_value(),
            password=info.pat.get_secret_value(),
        )

    @staticmethod
    def get_clickhouse_connection(info: ClickHouseConnectionInfo) -> BaseBackend:
        return ibis.clickhouse.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=(info.password and info.password.get_secret_value()),
            settings=info.settings if info.settings else dict(),
            **info.kwargs if info.kwargs else dict(),
        )

    @classmethod
    def get_mssql_connection(cls, info: MSSqlConnectionInfo) -> BaseBackend:
        return ibis.mssql.connect(
            host=info.host.get_secret_value(),
            port=info.port.get_secret_value(),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=info.password.get_secret_value(),
            driver=info.driver,
            TDS_Version=info.tds_version,
            **info.kwargs if info.kwargs else dict(),
        )

    @classmethod
    def get_mysql_connection(cls, info: MySqlConnectionInfo) -> BaseBackend:
        ssl_context = cls._create_ssl_context(info)
        kwargs = {"ssl": ssl_context} if ssl_context else {}

        # utf8mb4 is the actual charset used by MySQL for utf8
        kwargs.setdefault("charset", "utf8mb4")

        if info.kwargs:
            kwargs.update(info.kwargs)
        return ibis.mysql.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=info.password.get_secret_value() if info.password else "",
            **kwargs,
        )

    @staticmethod
    def get_postgres_connection(info: PostgresConnectionInfo) -> BaseBackend:
        return ibis.postgres.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=(info.password and info.password.get_secret_value()),
            **info.kwargs if info.kwargs else dict(),
        )

    @staticmethod
    def get_oracle_connection(info: OracleConnectionInfo) -> BaseBackend:
        # if dsn is provided, use it to connect
        # otherwise, use host, port, database, user, password, and sid
        if hasattr(info, "dsn") and info.dsn:
            return ibis.oracle.connect(
                dsn=info.dsn.get_secret_value(),
                user=info.user.get_secret_value(),
                password=(info.password and info.password.get_secret_value()),
            )
        return ibis.oracle.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=(info.password and info.password.get_secret_value()),
        )

    @staticmethod
    def get_snowflake_connection(info: SnowflakeConnectionInfo) -> BaseBackend:
        if hasattr(info, "private_key") and info.private_key:
            return ibis.snowflake.connect(
                user=info.user.get_secret_value(),
                account=info.account.get_secret_value(),
                database=info.database.get_secret_value(),
                schema=info.sf_schema.get_secret_value(),
                warehouse=info.warehouse.get_secret_value(),
                private_key=info.private_key.get_secret_value(),
                **info.kwargs if info.kwargs else dict(),
            )
        else:
            return ibis.snowflake.connect(
                user=info.user.get_secret_value(),
                password=info.password.get_secret_value(),
                account=info.account.get_secret_value(),
                database=info.database.get_secret_value(),
                schema=info.sf_schema.get_secret_value(),
                **info.kwargs if info.kwargs else dict(),
            )

    @staticmethod
    def get_trino_connection(info: TrinoConnectionInfo) -> BaseBackend:
        return ibis.trino.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.catalog.get_secret_value(),
            schema=info.trino_schema.get_secret_value(),
            user=(info.user and info.user.get_secret_value()),
            password=(info.password and info.password.get_secret_value()),
            **info.kwargs if info.kwargs else dict(),
        )

    @staticmethod
    def _create_ssl_context(info: ConnectionInfo) -> ssl.SSLContext | None:
        ssl_mode = (
            info.ssl_mode.get_secret_value()
            if hasattr(info, "ssl_mode") and info.ssl_mode
            else None
        )

        if ssl_mode == SSLMode.VERIFY_CA and not info.ssl_ca:
            raise ValueError("SSL CA must be provided when SSL mode is VERIFY CA")

        if not ssl_mode or ssl_mode == SSLMode.DISABLED:
            return None

        ctx = ssl.create_default_context()
        ctx.check_hostname = False

        if ssl_mode == SSLMode.ENABLED:
            ctx.verify_mode = ssl.CERT_NONE
        elif ssl_mode == SSLMode.VERIFY_CA:
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.load_verify_locations(
                cadata=base64.b64decode(info.ssl_ca.get_secret_value()).decode("utf-8")
                if info.ssl_ca
                else None
            )

        return ctx
