import pandas as pd

from app.model import ConnectionInfo, UnprocessableEntityError
from app.model.data_source import DataSource


class Connector:
    def __init__(
        self,
        data_source: DataSource,
        connection_info: ConnectionInfo,
        manifest_str: str,
    ):
        self.data_source = data_source
        self.connection = self.data_source.get_connection(connection_info)
        self.manifest_str = manifest_str

    def query(self, sql: str, limit: int) -> pd.DataFrame:
        return self.connection.sql(sql).limit(limit).to_pandas()

    def dry_run(self, sql: str) -> None:
        try:
            self.connection.sql(sql)
        except Exception as e:
            raise QueryDryRunError(f"Exception: {type(e)}, message: {e!s}")


class QueryDryRunError(UnprocessableEntityError):
    pass
