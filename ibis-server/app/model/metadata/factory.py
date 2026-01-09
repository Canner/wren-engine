from app.model.data_source import DataSource
from app.model.metadata.athena import AthenaMetadata
from app.model.metadata.bigquery import BigQueryMetadata
from app.model.metadata.canner import CannerMetadata
from app.model.metadata.clickhouse import ClickHouseMetadata
from app.model.metadata.databricks import DatabricksMetadata
from app.model.metadata.metadata import Metadata
from app.model.metadata.mssql import MSSQLMetadata
from app.model.metadata.mysql import MySQLMetadata
from app.model.metadata.object_storage import (
    DuckDBMetadata,
    GcsFileMetadata,
    LocalFileMetadata,
    MinioFileMetadata,
    S3FileMetadata,
)
from app.model.metadata.oracle import OracleMetadata
from app.model.metadata.postgres import PostgresMetadata
from app.model.metadata.redshift import RedshiftMetadata
from app.model.metadata.snowflake import SnowflakeMetadata
from app.model.metadata.spark import SparkMetadata
from app.model.metadata.trino import TrinoMetadata

mapping = {
    DataSource.athena: AthenaMetadata,
    DataSource.bigquery: BigQueryMetadata,
    DataSource.canner: CannerMetadata,
    DataSource.clickhouse: ClickHouseMetadata,
    DataSource.mssql: MSSQLMetadata,
    DataSource.mysql: MySQLMetadata,
    DataSource.oracle: OracleMetadata,
    DataSource.postgres: PostgresMetadata,
    DataSource.redshift: RedshiftMetadata,
    DataSource.trino: TrinoMetadata,
    DataSource.snowflake: SnowflakeMetadata,
    DataSource.local_file: LocalFileMetadata,
    DataSource.s3_file: S3FileMetadata,
    DataSource.minio_file: MinioFileMetadata,
    DataSource.gcs_file: GcsFileMetadata,
    DataSource.databricks: DatabricksMetadata,
    DataSource.spark: SparkMetadata,
}


class MetadataFactory:
    @staticmethod
    def get_metadata(data_source: DataSource, connection_info) -> Metadata:
        try:
            if (
                data_source
                in [
                    DataSource.local_file,
                    DataSource.s3_file,
                    DataSource.minio_file,
                    DataSource.gcs_file,
                ]
                and connection_info.format == "duckdb"
            ):
                # DuckDBMetadata is used for local file, S3, Minio, and GCS with DuckDB format
                return DuckDBMetadata(connection_info)

            return mapping[data_source](connection_info)
        except KeyError:
            raise NotImplementedError(f"Unsupported data source: {data_source}")
