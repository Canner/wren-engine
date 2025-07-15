import re
from urllib.parse import urlparse

from app.model import TrinoConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata


class TrinoMetadata(Metadata):
    def __init__(self, connection_info: TrinoConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.trino.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        schema = self._get_schema_name()
        sql = f"""
                SELECT
                    t.table_catalog,
                    t.table_schema,
                    t.table_name,
                    tc.comment AS table_comment,
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_comment
                FROM
                    information_schema.tables AS t
                INNER JOIN
                    information_schema.columns AS c
                    ON t.table_catalog = c.table_catalog
                    AND t.table_schema = c.table_schema
                    AND t.table_name = c.table_name
                INNER JOIN
                    system.metadata.table_comments AS tc
                    ON t.table_catalog = tc.catalog_name
                    AND t.table_schema = tc.schema_name
                    AND t.table_name = tc.table_name
                WHERE t.table_schema = '{schema}'
                AND c.table_catalog = (SELECT current_catalog)
                """
        response = self.connection.sql(sql).to_pandas().to_dict(orient="records")
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
                    description=row["table_comment"],
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
                    description=row["column_comment"],
                    properties=None,
                )
            )
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        return []

    def get_version(self) -> str:
        return self.connection.sql("SELECT version()").to_pandas().iloc[0, 0]

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
        # trim the (all characters) at the end of the data_type if exists
        data_type = re.sub(r"\(.*\)", "", data_type).strip()
        switcher = {
            # String Types (ignore Binary and Spatial Types for now)
            "char": RustWrenEngineColumnType.CHAR,
            "varchar": RustWrenEngineColumnType.VARCHAR,
            "tinytext": RustWrenEngineColumnType.TEXT,
            "text": RustWrenEngineColumnType.TEXT,
            "mediumtext": RustWrenEngineColumnType.TEXT,
            "longtext": RustWrenEngineColumnType.TEXT,
            "enum": RustWrenEngineColumnType.VARCHAR,
            "set": RustWrenEngineColumnType.VARCHAR,
            # Numeric Types(https://dev.mysql.com/doc/refman/8.4/en/numeric-types.html)
            "bit": RustWrenEngineColumnType.TINYINT,
            "tinyint": RustWrenEngineColumnType.TINYINT,
            "smallint": RustWrenEngineColumnType.SMALLINT,
            "mediumint": RustWrenEngineColumnType.INTEGER,
            "int": RustWrenEngineColumnType.INTEGER,
            "integer": RustWrenEngineColumnType.INTEGER,
            "bigint": RustWrenEngineColumnType.BIGINT,
            # boolean
            "bool": RustWrenEngineColumnType.BOOL,
            "boolean": RustWrenEngineColumnType.BOOL,
            # Decimal
            "float": RustWrenEngineColumnType.FLOAT4,
            "double": RustWrenEngineColumnType.DOUBLE,
            "decimal": RustWrenEngineColumnType.DECIMAL,
            "numeric": RustWrenEngineColumnType.NUMERIC,
            # Date and Time Types(https://dev.mysql.com/doc/refman/8.4/en/date-and-time-types.html)
            "date": RustWrenEngineColumnType.DATE,
            "datetime": RustWrenEngineColumnType.TIMESTAMP,
            "timestamp": RustWrenEngineColumnType.TIMESTAMPTZ,
            # JSON Type
            "json": RustWrenEngineColumnType.JSON,
        }

        return switcher.get(data_type.lower(), RustWrenEngineColumnType.UNKNOWN)
