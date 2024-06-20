from __future__ import annotations

import base64
from enum import Enum, StrEnum, auto
from json import loads

import ibis
from google.oauth2 import service_account
from ibis import BaseBackend

from app.model import (
    BigQueryConnectionInfo,
    ConnectionInfo,
    ConnectionUrl,
    MySqlConnectionInfo,
    PostgresConnectionInfo,
    QueryBigQueryDTO,
    QueryDTO,
    QueryMySqlDTO,
    QueryPostgresDTO,
    QuerySnowflakeDTO,
    SnowflakeConnectionInfo,
)


class DataSource(StrEnum):
    bigquery = auto()
    mysql = auto()
    postgres = auto()
    snowflake = auto()

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
    mysql = QueryMySqlDTO
    postgres = QueryPostgresDTO
    snowflake = QuerySnowflakeDTO

    def __init__(self, dto: QueryDTO):
        self.dto = dto

    def get_connection(self, info: ConnectionInfo) -> BaseBackend:
        try:
            return getattr(self, f"get_{self.name}_connection")(info)
        except KeyError:
            raise NotImplementedError(f"Unsupported data source: {self}")

    @staticmethod
    def get_bigquery_connection(info: BigQueryConnectionInfo) -> BaseBackend:
        credits_json = loads(base64.b64decode(info.credentials).decode("utf-8"))
        credentials = service_account.Credentials.from_service_account_info(
            credits_json
        )
        return ibis.bigquery.connect(
            project_id=info.project_id,
            dataset_id=info.dataset_id,
            credentials=credentials,
        )

    @staticmethod
    def get_mysql_connection(
        info: ConnectionUrl | MySqlConnectionInfo,
    ) -> BaseBackend:
        return ibis.connect(
            getattr(info, "connection_url", None)
            or f"mysql://{info.user}:{info.password}@{info.host}:{info.port}/{info.database}",
            port=info.port,  # ibis miss port of connection url, so we need to pass it explicitly
        )

    @staticmethod
    def get_postgres_connection(
        info: ConnectionUrl | PostgresConnectionInfo,
    ) -> BaseBackend:
        return ibis.connect(
            getattr(info, "connection_url", None)
            or f"postgres://{info.user}:{info.password}@{info.host}:{info.port}/{info.database}"
        )

    @staticmethod
    def get_snowflake_connection(info: SnowflakeConnectionInfo) -> BaseBackend:
        return ibis.snowflake.connect(
            user=info.user,
            password=info.password,
            account=info.account,
            database=info.database,
            schema=info.sf_schema,
        )
