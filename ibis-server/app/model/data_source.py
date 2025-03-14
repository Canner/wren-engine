from __future__ import annotations

import base64
import ssl
from enum import Enum, StrEnum, auto
from json import loads
from typing import Optional

import ibis
from google.oauth2 import service_account
from ibis import BaseBackend

from app.model import (
    BigQueryConnectionInfo,
    CannerConnectionInfo,
    ClickHouseConnectionInfo,
    ConnectionInfo,
    MSSqlConnectionInfo,
    MySqlConnectionInfo,
    OracleConnectionInfo,
    PostgresConnectionInfo,
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
    QueryS3FileDTO,
    QuerySnowflakeDTO,
    QueryTrinoDTO,
    SnowflakeConnectionInfo,
    SSLMode,
    TrinoConnectionInfo,
)


class DataSource(StrEnum):
    bigquery = auto()
    canner = auto()
    clickhouse = auto()
    mssql = auto()
    mysql = auto()
    oracle = auto()
    postgres = auto()
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


class DataSourceExtension(Enum):
    bigquery = QueryBigQueryDTO
    canner = QueryCannerDTO
    clickhouse = QueryClickHouseDTO
    mssql = QueryMSSqlDTO
    mysql = QueryMySqlDTO
    oracle = QueryOracleDTO
    postgres = QueryPostgresDTO
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
                return ibis.connect(info.connection_url.get_secret_value())
            if self.name == "local_file":
                raise NotImplementedError(
                    "Local file connection is not implemented to get ibis backend"
                )
            return getattr(self, f"get_{self.name}_connection")(info)
        except KeyError:
            raise NotImplementedError(f"Unsupported data source: {self}")

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
            password=(info.password and info.password.get_secret_value()),
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
        )

    @staticmethod
    def get_oracle_connection(info: OracleConnectionInfo) -> BaseBackend:
        return ibis.oracle.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=(info.password and info.password.get_secret_value()),
        )

    @staticmethod
    def get_snowflake_connection(info: SnowflakeConnectionInfo) -> BaseBackend:
        return ibis.snowflake.connect(
            user=info.user.get_secret_value(),
            password=info.password.get_secret_value(),
            account=info.account.get_secret_value(),
            database=info.database.get_secret_value(),
            schema=info.sf_schema.get_secret_value(),
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
        )

    @staticmethod
    def _create_ssl_context(info: ConnectionInfo) -> Optional[ssl.SSLContext]:
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
