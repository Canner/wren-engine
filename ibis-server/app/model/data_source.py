from __future__ import annotations

import base64
from enum import StrEnum, auto
from json import loads
from typing import Union

import ibis
from google.oauth2 import service_account
from ibis import BaseBackend
from pydantic import BaseModel, Field


class DataSource(StrEnum):
    postgres = auto()
    bigquery = auto()
    snowflake = auto()

    def get_connection(self, dto) -> BaseBackend:
        match self:
            case DataSource.postgres:
                return self.get_postgres_connection(dto)
            case DataSource.bigquery:
                return self.get_bigquery_connection(dto)
            case DataSource.snowflake:
                return self.get_snowflake_connection(dto)
            case _:
                raise NotImplementedError(f'Unsupported data source: {self}')

    @staticmethod
    def get_postgres_connection(info: PostgresConnectionUrl | PostgresConnectionInfo) -> BaseBackend:
        return ibis.connect(getattr(info, 'connection_url', None) or f"postgres://{info.user}:{info.password}@{info.host}:{info.port}/{info.database}")

    @staticmethod
    def get_bigquery_connection(info: BigQueryConnectionInfo) -> BaseBackend:
        credits_json = loads(base64.b64decode(info.credentials).decode('utf-8'))
        credentials = service_account.Credentials.from_service_account_info(credits_json)
        return ibis.bigquery.connect(
            project_id=info.project_id,
            dataset_id=info.dataset_id,
            credentials=credentials,
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


class PostgresConnectionInfo(BaseModel):
    host: str = Field(examples=["localhost"])
    port: int = Field(examples=[5432])
    database: str
    user: str
    password: str


class PostgresConnectionUrl(BaseModel):
    connection_url: str = Field(alias="connectionUrl")


class BigQueryConnectionInfo(BaseModel):
    project_id: str
    dataset_id: str
    credentials: str = Field(description="Base64 encode `credentials.json`")


class SnowflakeConnectionInfo(BaseModel):
    user: str
    password: str
    account: str
    database: str
    sf_schema: str = Field(alias="schema", default=None)  # Use `sf_schema` to avoid `schema` shadowing in BaseModel


ConnectionInfo = Union[
    PostgresConnectionInfo,
    BigQueryConnectionInfo,
    SnowflakeConnectionInfo,
]
