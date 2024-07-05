import pandas as pd

from app.mdl.rewriter import Rewriter
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
        rewritten_sql = Rewriter(self.manifest_str, self.data_source).rewrite(sql)
        return (
            self.connection.sql(
                rewritten_sql,
            )
            .limit(limit)
            .to_pandas()
        )

    def dry_run(self, sql: str) -> None:
        try:
            rewritten_sql = Rewriter(self.manifest_str, self.data_source).rewrite(sql)
            self.connection.sql(rewritten_sql)
        except Exception as e:
            raise QueryDryRunError(f"Exception: {type(e)}, message: {e!s}")


class QueryDryRunError(Exception):
    pass
