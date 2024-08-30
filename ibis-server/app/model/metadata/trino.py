from json import loads
from urllib.parse import urlparse

import pandas as pd

from app.model import ConnectionUrl, TrinoConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    Table,
    TableProperties,
    WrenEngineColumnType,
)
from app.model.metadata.metadata import Metadata


class TrinoMetadata(Metadata):
    def __init__(self, connection_info: TrinoConnectionInfo | ConnectionUrl):
        super().__init__(connection_info)

    def get_table_list(self) -> list[Table]:
        schema = self._get_schema_name()
        sql = f"""SELECT
                t.table_catalog,
                t.table_schema,
                t.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable
              FROM
                information_schema.tables t
              JOIN
                information_schema.columns c 
              ON t.table_schema = c.table_schema 
              AND t.table_name = c.table_name
              WHERE
                t.table_type IN ('BASE TABLE', 'VIEW')
              AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
              AND t.table_schema = '{schema}'"""

        sql_cursor = DataSource.trino.get_connection(self.connection_info).raw_sql(sql)
        column_names = [col[0] for col in sql_cursor.description]
        response = loads(
            pd.DataFrame(sql_cursor.fetchall(), columns=column_names).to_json(
                orient="records"
            )
        )
        unique_tables = {}
        for row in response:
            # generate unique table name
            schema_table = self._format_trino_compact_table_name(
                row["table_catalog"], row["table_schema"], row["table_name"]
            )
            # init table if not exists
            if schema_table not in unique_tables:
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description="",
                    columns=[],
                    properties=TableProperties(
                        schema=row["table_schema"],
                        catalog=row["table_catalog"],
                        table=row["table_name"],
                    ),
                    primaryKey="",
                )

            # table exists, and add column to the table
            unique_tables[schema_table].columns.append(
                Column(
                    name=row["column_name"],
                    type=self._transform_column_type(row["data_type"]),
                    notNull=row["is_nullable"].lower() == "no",
                    description="",
                    properties=None,
                )
            )
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        return []

    def _format_trino_compact_table_name(
        self, catalog: str, schema: str, table: str
    ) -> str:
        return f"{catalog}.{schema}.{table}"

    def _get_schema_name(self):
        if hasattr(self.connection_info, "connection_url"):
            return urlparse(
                self.connection_info.connection_url.get_secret_value()
            ).path.split("/")[-1]
        else:
            return self.connection_info.trino_schema.get_secret_value()

    def _transform_column_type(self, data_type):
        # all possible types listed here: https://trino.io/docs/current/language/types.html
        switcher = {
            # String Types (ignore Binary and Spatial Types for now)
            "char": WrenEngineColumnType.CHAR,
            "varchar": WrenEngineColumnType.VARCHAR,
            "tinytext": WrenEngineColumnType.TEXT,
            "text": WrenEngineColumnType.TEXT,
            "mediumtext": WrenEngineColumnType.TEXT,
            "longtext": WrenEngineColumnType.TEXT,
            "enum": WrenEngineColumnType.VARCHAR,
            "set": WrenEngineColumnType.VARCHAR,
            # Numeric Types(https://dev.mysql.com/doc/refman/8.4/en/numeric-types.html)
            "bit": WrenEngineColumnType.TINYINT,
            "tinyint": WrenEngineColumnType.TINYINT,
            "smallint": WrenEngineColumnType.SMALLINT,
            "mediumint": WrenEngineColumnType.INTEGER,
            "int": WrenEngineColumnType.INTEGER,
            "integer": WrenEngineColumnType.INTEGER,
            "bigint": WrenEngineColumnType.BIGINT,
            # boolean
            "bool": WrenEngineColumnType.BOOLEAN,
            "boolean": WrenEngineColumnType.BOOLEAN,
            # Decimal
            "float": WrenEngineColumnType.FLOAT8,
            "double": WrenEngineColumnType.DOUBLE,
            "decimal": WrenEngineColumnType.DECIMAL,
            "numeric": WrenEngineColumnType.NUMERIC,
            # Date and Time Types(https://dev.mysql.com/doc/refman/8.4/en/date-and-time-types.html)
            "date": WrenEngineColumnType.DATE,
            "datetime": WrenEngineColumnType.TIMESTAMP,
            "timestamp": WrenEngineColumnType.TIMESTAMPTZ,
            # JSON Type
            "json": WrenEngineColumnType.JSON,
        }

        return switcher.get(data_type.lower(), WrenEngineColumnType.UNKNOWN)
