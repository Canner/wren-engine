from json import loads

import pandas as pd

from app.mdl.rewriter import rewrite
from app.model.data_source import DataSource, ConnectionInfo


class Connector:
    def __init__(self, data_source: DataSource, connection_info: ConnectionInfo, manifest_str: str, column_dtypes: dict[str, str]):
        self.data_source = data_source
        self.connection = self.data_source.get_connection(connection_info)
        self.manifest_str = manifest_str
        self.column_dtypes = column_dtypes

    def query(self, sql) -> dict:
        rewritten_sql = rewrite(self.manifest_str, sql)
        return self._to_json(self.connection.sql(rewritten_sql, dialect='trino').to_pandas())

    def dry_run(self, sql) -> None:
        try:
            rewritten_sql = rewrite(self.manifest_str, sql)
            self.connection.sql(rewritten_sql, dialect='trino')
        except Exception as e:
            raise QueryDryRunError(f'Exception: {type(e)}, message: {str(e)}')

    def _to_json(self, df):
        if self.column_dtypes:
            self._to_specific_types(df, self.column_dtypes)
        json_obj = loads(df.to_json(orient='split'))
        del json_obj['index']
        json_obj['dtypes'] = df.dtypes.apply(lambda x: x.name).to_dict()
        return json_obj

    def _to_specific_types(self, df: pd.DataFrame, column_dtypes: dict[str, str]):
        for column, dtype in column_dtypes.items():
            if dtype == 'datetime64':
                df[column] = self._to_datetime_and_format(df[column])
            else:
                df[column] = df[column].astype(dtype)

    @staticmethod
    def _to_datetime_and_format(series: pd.Series) -> pd.Series:
        series = pd.to_datetime(series, errors='coerce')
        return series.apply(lambda d: d.strftime('%Y-%m-%d %H:%M:%S.%f' + (' %Z' if series.dt.tz is not None else '')) if not pd.isnull(d) else d)


class QueryDryRunError(Exception):
    pass
