from json import loads

from app.model import ConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.bigquery import BigQueryMetadata
from app.model.metadata.clickhouse import ClickHouseMetadata
from app.model.metadata.dto import (
    Constraint,
    Table,
)
from app.model.metadata.metadata import Metadata
from app.model.metadata.mssql import MSSQLMetadata
from app.model.metadata.mysql import MySQLMetadata
from app.model.metadata.postgres import PostgresMetadata


class MetadataFactory:
    def __init__(self, data_source: DataSource, connection_info: ConnectionInfo):
        self.metadata = self.get_metadata(data_source, connection_info)

    def get_metadata(self, data_source: DataSource, connection_info) -> Metadata:
        if data_source == DataSource.postgres:
            return PostgresMetadata(connection_info)
        if data_source == DataSource.bigquery:
            return BigQueryMetadata(connection_info)
        if data_source == DataSource.mysql:
            return MySQLMetadata(connection_info)
        if data_source == DataSource.mssql:
            return MSSQLMetadata(connection_info)
        if data_source == DataSource.clickhouse:
            return ClickHouseMetadata(connection_info)

        raise NotImplementedError(f"Unsupported data source: {self}")

    def get_table_list(self) -> list[Table]:
        return self.metadata.get_table_list()

    def get_constraints(self) -> list[Constraint]:
        return self.metadata.get_constraints()


def to_json(df):
    json_obj = loads(df.to_json(orient="split"))
    del json_obj["index"]
    json_obj["dtypes"] = df.dtypes.apply(lambda x: x.name).to_dict()
    return json_obj
