from loguru import logger

from app.model import MySqlConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    ConstraintType,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

# MySQL-specific type mapping
# All possible types listed here: https://dev.mysql.com/doc/refman/8.4/en/data-types.html
MYSQL_TYPE_MAPPING = {
    # String Types (ignore Binary and Spatial Types for now)
    "char": RustWrenEngineColumnType.CHAR,
    "varchar": RustWrenEngineColumnType.VARCHAR,
    "tinytext": RustWrenEngineColumnType.TEXT,
    "text": RustWrenEngineColumnType.TEXT,
    "mediumtext": RustWrenEngineColumnType.TEXT,
    "longtext": RustWrenEngineColumnType.TEXT,
    "enum": RustWrenEngineColumnType.VARCHAR,
    "set": RustWrenEngineColumnType.VARCHAR,
    # Numeric Types (https://dev.mysql.com/doc/refman/8.4/en/numeric-types.html)
    "bit": RustWrenEngineColumnType.TINYINT,
    "tinyint": RustWrenEngineColumnType.TINYINT,
    "smallint": RustWrenEngineColumnType.SMALLINT,
    "mediumint": RustWrenEngineColumnType.INTEGER,
    "int": RustWrenEngineColumnType.INTEGER,
    "integer": RustWrenEngineColumnType.INTEGER,
    "bigint": RustWrenEngineColumnType.BIGINT,
    # Boolean Types
    "bool": RustWrenEngineColumnType.BOOL,
    "boolean": RustWrenEngineColumnType.BOOL,
    # Decimal Types
    "float": RustWrenEngineColumnType.FLOAT8,
    "double": RustWrenEngineColumnType.DOUBLE,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "numeric": RustWrenEngineColumnType.NUMERIC,
    # Date and Time Types (https://dev.mysql.com/doc/refman/8.4/en/date-and-time-types.html)
    "date": RustWrenEngineColumnType.DATE,
    "datetime": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp": RustWrenEngineColumnType.TIMESTAMPTZ,
    # JSON Type
    "json": RustWrenEngineColumnType.JSON,
}


class MySQLMetadata(Metadata):
    def __init__(self, connection_info: MySqlConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.mysql.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                c.TABLE_SCHEMA AS table_schema,
                c.TABLE_NAME AS table_name,
                c.COLUMN_NAME AS column_name,
                c.DATA_TYPE AS data_type,
                c.IS_NULLABLE AS is_nullable,
                c.COLUMN_KEY AS column_key,
                c.COLUMN_COMMENT AS column_comment,
                t.TABLE_COMMENT AS table_comment
            FROM
                information_schema.COLUMNS c
            JOIN
                information_schema.TABLES t
                ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
                AND c.TABLE_NAME = t.TABLE_NAME
            WHERE
                c.TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
            """
        response = self.connection.sql(sql).to_pandas().to_dict(orient="records")

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
                    description=row["table_comment"],
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
                    description=row["column_comment"],
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
        res = self.connection.sql(sql).to_pandas().to_dict(orient="records")
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

    def get_version(self) -> str:
        return self.connection.sql("SELECT version()").to_pandas().iloc[0, 0]

    def _format_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _format_constraint_name(
        self, table_name, column_name, referenced_table_name, referenced_column_name
    ):
        return f"{table_name}_{column_name}_{referenced_table_name}_{referenced_column_name}"

    def _transform_column_type(self, data_type: str) -> RustWrenEngineColumnType:
        """Transform MySQL data type to RustWrenEngineColumnType.

        Args:
            data_type: The MySQL data type string

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        # Remove parameter specifications like VARCHAR(255) -> VARCHAR
        normalized_type = data_type.strip().lower()

        # Use the module-level mapping table
        mapped_type = MYSQL_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown MySQL data type: {data_type}")

        return mapped_type
