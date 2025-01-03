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
    PostgresConnectionInfo,
    QueryBigQueryDTO,
    QueryCannerDTO,
    QueryClickHouseDTO,
    QueryDTO,
    QueryLocalFileDTO,
    QueryMSSqlDTO,
    QueryMySqlDTO,
    QueryPostgresDTO,
    QuerySnowflakeDTO,
    QueryTrinoDTO,
    SnowflakeConnectionInfo,
    TrinoConnectionInfo,
)


class SSLMode(str, Enum):
    DISABLE = "Disable"
    REQUIRE = "Require"
    VERIFY_CA = "Verify CA"


class DataSource(StrEnum):
    bigquery = auto()
    canner = auto()
    clickhouse = auto()
    mssql = auto()
    mysql = auto()
    postgres = auto()
    snowflake = auto()
    trino = auto()
    local_file = auto()

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
    postgres = QueryPostgresDTO
    snowflake = QuerySnowflakeDTO
    trino = QueryTrinoDTO
    local_file = QueryLocalFileDTO

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
            password=info.password.get_secret_value(),
        )

    @classmethod
    def get_mssql_connection(cls, info: MSSqlConnectionInfo) -> BaseBackend:
        return ibis.mssql.connect(
            host=info.host.get_secret_value(),
            port=info.port.get_secret_value(),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=cls._escape_special_characters_for_odbc(
                info.password.get_secret_value()
            ),
            driver=info.driver,
            TDS_Version=info.tds_version,
            **info.kwargs if info.kwargs else dict(),
        )

    @classmethod
    def get_mysql_connection(cls, info: MySqlConnectionInfo) -> BaseBackend:
        ssl_context = cls._create_ssl_context(info)
        kwargs = {"ssl": ssl_context} if ssl_context else {}
        if info.kwargs:
            kwargs.update(info.kwargs)
        return ibis.mysql.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=info.password.get_secret_value(),
            **kwargs,
        )

    @staticmethod
    def get_postgres_connection(info: PostgresConnectionInfo) -> BaseBackend:
        return ibis.postgres.connect(
            host=info.host.get_secret_value(),
            port=int(info.port.get_secret_value()),
            database=info.database.get_secret_value(),
            user=info.user.get_secret_value(),
            password=info.password.get_secret_value(),
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
    def _escape_special_characters_for_odbc(value: str) -> str:
        return "{" + value.replace("}", "}}") + "}"

    @staticmethod
    def _create_ssl_context(info: ConnectionInfo) -> Optional[ssl.SSLContext]:
        ssl_mode = info.ssl_mode.get_secret_value()

        if ssl_mode == SSLMode.DISABLE:
            return None

        ctx = ssl.create_default_context()
        ctx.check_hostname = False

        if ssl_mode == SSLMode.REQUIRE:
            ctx.verify_mode = ssl.CERT_NONE
        elif ssl_mode == SSLMode.VERIFY_CA:
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.load_verify_locations(
                cadata=base64.b64decode(info.ssl_ca.get_secret_value()).decode("utf-8")
                if info.ssl_ca
                else None
            )

        return ctx
