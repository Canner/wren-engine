import base64
from enum import StrEnum
from json import loads

import ibis
from google.oauth2 import service_account

from dto import PostgresDTO, BigQueryDTO, SnowflakeDTO


class DataSource(StrEnum):
    postgres = "postgres"
    bigquery = "bigquery"
    snowflake = "snowflake"

    def get_connection(self, dto):
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
    def get_postgres_connection(dto: PostgresDTO):
        return ibis.postgres.connect(
            user=dto.user,
            password=dto.password,
            host=dto.host,
            port=dto.port,
        )

    @staticmethod
    def get_bigquery_connection(dto: BigQueryDTO):
        credits_json = loads(base64.b64decode(dto.credentials).decode('utf-8'))
        credentials = service_account.Credentials.from_service_account_info(credits_json)
        return ibis.bigquery.connect(
            project_id=dto.project_id,
            dataset_id=dto.dataset_id,
            credentials=credentials,
        )

    @staticmethod
    def get_snowflake_connection(dto: SnowflakeDTO):
        return ibis.snowflake.connect(
            user=dto.user,
            password=dto.password,
            account=dto.account,
            database=dto.database,
            schema=dto.sf_schema,
        )
