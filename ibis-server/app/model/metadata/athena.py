import re
from contextlib import closing

import pandas as pd
from loguru import logger

from app.model import AthenaConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

# Athena-specific type mapping
ATHENA_TYPE_MAPPING = {
    # String Types (ignore Binary and Spatial Types for now)
    "char": RustWrenEngineColumnType.CHAR,
    "varchar": RustWrenEngineColumnType.VARCHAR,
    "tinytext": RustWrenEngineColumnType.TEXT,
    "text": RustWrenEngineColumnType.TEXT,
    "mediumtext": RustWrenEngineColumnType.TEXT,
    "longtext": RustWrenEngineColumnType.TEXT,
    "enum": RustWrenEngineColumnType.VARCHAR,
    "set": RustWrenEngineColumnType.VARCHAR,
    # Integer Types
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
    "float": RustWrenEngineColumnType.FLOAT4,
    "double": RustWrenEngineColumnType.DOUBLE,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "numeric": RustWrenEngineColumnType.NUMERIC,
    # Date/Time Types
    "date": RustWrenEngineColumnType.DATE,
    "datetime": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp": RustWrenEngineColumnType.TIMESTAMPTZ,
    # JSON Type
    "json": RustWrenEngineColumnType.JSON,
}


class AthenaMetadata(Metadata):
    def __init__(self, connection_info: AthenaConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.athena.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        schema_name = self.connection_info.schema_name.get_secret_value()

        sql = f"""
            SELECT 
                t.table_catalog,
                t.table_schema,
                t.table_name,
                c.column_name,
                c.comment,
                c.ordinal_position,
                c.is_nullable,
                c.data_type
            FROM 
                information_schema.tables AS t 
            JOIN 
                information_schema.columns AS c 
                ON t.table_catalog = c.table_catalog
                AND t.table_schema = c.table_schema
                AND t.table_name = c.table_name
            WHERE t.table_schema = '{schema_name}'
            ORDER BY t.table_name
            """

        # We need to use raw_sql here because using the sql method causes Ibis to *create  view* first,
        # which does not work with information_schema queries.
        with closing(self.connection.raw_sql(sql)) as cursor:
            response = pd.DataFrame(
                cursor.fetchall(), columns=[col[0] for col in cursor.description]
            ).to_dict(orient="records")

        def get_column(row) -> Column:
            return Column(
                name=row["column_name"],
                type=self._transform_column_type(row["data_type"]),
                notNull=row["is_nullable"].lower() == "no",
                description=row["comment"],
                properties=None,
            )

        def get_table(row) -> Table:
            return Table(
                name=self._format_athena_compact_table_name(
                    row["table_schema"], row["table_name"]
                ),
                description="",  # Athena doesn't provide table descriptions in information_schema
                columns=[],
                properties=TableProperties(
                    schema=row["table_schema"],
                    catalog=row["table_catalog"],
                    table=row["table_name"],
                ),
                primaryKey="",
            )

        unique_tables = {}

        for column_metadata in response:
            # generate unique table name
            table_name = self._format_athena_compact_table_name(
                column_metadata["table_schema"], column_metadata["table_name"]
            )
            # init table if not exists
            if table_name not in unique_tables:
                unique_tables[table_name] = get_table(column_metadata)

            current_table = unique_tables[table_name]
            # add column to table
            current_table.columns.append(get_column(column_metadata))

        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        # Athena doesn't support foreign key constraints
        return []

    def get_version(self) -> str:
        return "AWS Athena - Follow AWS service versioning"

    def _format_athena_compact_table_name(self, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    def _transform_column_type(self, data_type: str) -> RustWrenEngineColumnType:
        """Transform Athena data type to RustWrenEngineColumnType.

        Args:
            data_type: The Athena data type string

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        # Remove parameter specifications like VARCHAR(255) -> VARCHAR
        normalized_type = re.sub(r"\(.*\)", "", data_type).strip().lower()

        # Use the module-level mapping table
        mapped_type = ATHENA_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown Athena data type: {data_type}")

        return mapped_type
