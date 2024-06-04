from json import loads

from app.mdl.rewriter import rewrite
from app.model.data_source import DataSource, ConnectionInfo


class Coordinator:
    def __init__(self, data_source: DataSource, connection_info: ConnectionInfo, manifest_str: str):
        self.data_source = data_source
        self.connection = self.data_source.get_connection(connection_info)
        self.manifest_str = manifest_str

    def query(self, sql) -> dict:
        rewritten_sql = rewrite(self.manifest_str, sql)
        return self._to_json(self.connection.sql(rewritten_sql, dialect='trino').to_pandas())

    def dry_run(self, sql) -> None:
        try:
            rewritten_sql = rewrite(self.manifest_str, sql)
            self.connection.sql(rewritten_sql, dialect='trino')
        except Exception as e:
            raise QueryDryRunError(f'Exception: {type(e)}, message: {str(e)}')

    @staticmethod
    def _to_json(df):
        json_obj = loads(df.to_json(orient='split'))
        del json_obj['index']
        json_obj['dtypes'] = df.dtypes.apply(lambda x: x.name).to_dict()
        return json_obj


class QueryDryRunError(Exception):
    pass
