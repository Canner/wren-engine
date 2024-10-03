from app.model.data_source import DataSource
from app.model.metadata.bigquery import BigQueryMetadata
from app.model.metadata.clickhouse import ClickHouseMetadata
from app.model.metadata.metadata import Metadata
from app.model.metadata.mssql import MSSQLMetadata
from app.model.metadata.mysql import MySQLMetadata
from app.model.metadata.postgres import PostgresMetadata
from app.model.metadata.trino import TrinoMetadata

mapping = {
    DataSource.bigquery: BigQueryMetadata,
    DataSource.clickhouse: ClickHouseMetadata,
    DataSource.mssql: MSSQLMetadata,
    DataSource.mysql: MySQLMetadata,
    DataSource.postgres: PostgresMetadata,
    DataSource.trino: TrinoMetadata,
}


class MetadataFactory:
    @staticmethod
    def get_metadata(data_source: DataSource, connection_info) -> Metadata:
        try:
            return mapping[data_source](connection_info)
        except KeyError:
            raise NotImplementedError(f"Unsupported data source: {data_source}")
