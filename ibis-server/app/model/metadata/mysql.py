from json import loads

from app.model import MySqlConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    ConstraintType,
    Table,
    TableProperties,
    WrenEngineColumnType,
)
from app.model.metadata.metadata import Metadata


class MySQLMetadata(Metadata):
    def __init__(self, connection_info: MySqlConnectionInfo):
        super().__init__(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                TABLE_SCHEMA as table_schema,
                TABLE_NAME as table_name,
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_KEY as column_key
            FROM
                information_schema.COLUMNS
            WHERE
                TABLE_SCHEMA not IN ("mysql", "information_schema", "performance_schema", "sys")
            """
        response = loads(
            DataSource.mysql.get_connection(self.connection_info)
            .sql(sql)
            .to_pandas()
            .to_json(orient="records")
        )

        unique_tables = {}
        for row in response:
            # generate unique table name
            schema_table = self._format_compact_table_name(
                row["table_schema"], row["table_name"]
            )
            # init table if not exists
            if schema_table not in unique_tables:
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description="",
                    columns=[],
                    properties=TableProperties(
                        schema=row["table_schema"],
                        # the catalog is all "def" and can not be used when query, return empty string
                        catalog="",
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
            # if column is primary key
            if row["column_key"] == "PRI":
                unique_tables[schema_table].primaryKey = row["column_name"]
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        # The REFERENTIAL_CONSTRAINTS table provides information about foreign keys.
        # The KEY_COLUMN_USAGE table describes which key columns have constraints.
        sql = """
            SELECT 
                kcu.CONSTRAINT_NAME as constraint_name,
                kcu.TABLE_SCHEMA AS table_schema,
                kcu.TABLE_NAME AS table_name,
                kcu.COLUMN_NAME AS column_name,
                kcu.REFERENCED_TABLE_SCHEMA AS referenced_table_schema,
                kcu.REFERENCED_TABLE_NAME AS referenced_table_name,
                kcu.REFERENCED_COLUMN_NAME AS referenced_column_name
            FROM 
                information_schema.REFERENTIAL_CONSTRAINTS rc
            JOIN 
                information_schema.KEY_COLUMN_USAGE kcu 
                ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
                AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            """
        res = loads(
            DataSource.mysql.get_connection(self.connection_info)
            .sql(sql)
            .to_pandas()
            .to_json(orient="records")
        )
        constraints = []
        for row in res:
            constraints.append(
                Constraint(
                    constraintName=self._format_constraint_name(
                        row["table_name"],
                        row["column_name"],
                        row["referenced_table_name"],
                        row["referenced_column_name"],
                    ),
                    constraintTable=self._format_compact_table_name(
                        row["table_schema"], row["table_name"]
                    ),
                    constraintColumn=row["column_name"],
                    constraintedTable=self._format_compact_table_name(
                        row["referenced_table_schema"], row["referenced_table_name"]
                    ),
                    constraintedColumn=row["referenced_column_name"],
                    constraintType=ConstraintType.FOREIGN_KEY,
                )
            )
        return constraints

    def _format_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _format_constraint_name(
        self, table_name, column_name, referenced_table_name, referenced_column_name
    ):
        return f"{table_name}_{column_name}_{referenced_table_name}_{referenced_column_name}"

    def _transform_column_type(self, data_type):
        # all possible types listed here: https://dev.mysql.com/doc/refman/8.4/en/data-types.html
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


def to_json(df):
    json_obj = loads(df.to_json(orient="split"))
    del json_obj["index"]
    return json_obj
