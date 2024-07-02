import pandas as pd

from app.mdl.rewriter import rewrite
from app.model import ConnectionInfo
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
        rewritten_sql = rewrite(self.manifest_str, sql)
        return (
            self.connection.sql(rewritten_sql, dialect="trino").limit(limit).to_pandas()
        )

    def dry_run(self, sql: str) -> None:
        try:
            rewritten_sql = rewrite(self.manifest_str, sql)
            self.connection.sql(rewritten_sql, dialect="trino")
        except Exception as e:
            raise QueryDryRunError(f"Exception: {type(e)}, message: {str(e)}")


class QueryDryRunError(Exception):
    pass
