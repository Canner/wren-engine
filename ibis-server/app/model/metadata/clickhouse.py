from loguru import logger

from app.model import ClickHouseConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

# ClickHouse-specific type mapping
CLICKHOUSE_TYPE_MAPPING = {
    # Boolean Types
    "boolean": RustWrenEngineColumnType.BOOL,
    # Integer Types
    "int8": RustWrenEngineColumnType.TINYINT,
    "uint8": RustWrenEngineColumnType.INT2,
    "int16": RustWrenEngineColumnType.INT2,
    "uint16": RustWrenEngineColumnType.INT2,
    "int32": RustWrenEngineColumnType.INT4,
    "uint32": RustWrenEngineColumnType.INT4,
    "int64": RustWrenEngineColumnType.INT8,
    "uint64": RustWrenEngineColumnType.INT8,
    # Float Types
    "float32": RustWrenEngineColumnType.FLOAT4,
    "float64": RustWrenEngineColumnType.FLOAT8,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "numeric": RustWrenEngineColumnType.NUMERIC,
    # Date/Time Types
    "date": RustWrenEngineColumnType.DATE,
    "datetime": RustWrenEngineColumnType.TIMESTAMP,
    # String Types
    "string": RustWrenEngineColumnType.VARCHAR,
    "fixedstring": RustWrenEngineColumnType.CHAR,
    # Special Types
    "uuid": RustWrenEngineColumnType.UUID,
    "enum8": RustWrenEngineColumnType.STRING,  # Enums can be mapped to strings
    "enum16": RustWrenEngineColumnType.STRING,  # Enums can be mapped to strings
    "ipv4": RustWrenEngineColumnType.INET,
    "ipv6": RustWrenEngineColumnType.INET,
}


class ClickHouseMetadata(Metadata):
    def __init__(self, connection_info: ClickHouseConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.clickhouse.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                c.database AS table_schema,
                c.table AS table_name,
                t.comment AS table_comment,
                c.name AS column_name,
                c.type AS data_type,
                c.comment AS column_comment
            FROM
                system.columns AS c
            JOIN
                system.tables AS t
                ON c.database = t.database
                AND c.table = t.name
            WHERE
                c.database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema', 'pg_catalog');
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
                        catalog=None,
                        schema=row["table_schema"],
                        table=row["table_name"],
                    ),
                    primaryKey="",
                )

            # table exists, and add column to the table
            unique_tables[schema_table].columns.append(
                Column(
                    name=row["column_name"],
                    type=self._transform_column_type(row["data_type"]),
                    notNull=False,
                    description=row["column_comment"],
                    properties=None,
                )
            )
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        return []

    def get_version(self) -> str:
        return self.connection.sql("SELECT version()").to_pandas().iloc[0, 0]

    def _format_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _transform_column_type(self, data_type: str) -> RustWrenEngineColumnType:
        """Transform ClickHouse data type to RustWrenEngineColumnType.

        Args:
            data_type: The ClickHouse data type string

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        # Convert to lowercase for comparison
        normalized_type = data_type.lower()

        # Decimal type with precision and scale
        if normalized_type.startswith("decimal"):
            return RustWrenEngineColumnType.DECIMAL

        # Numeric type with precision and scale
        if normalized_type.startswith("numeric"):
            return RustWrenEngineColumnType.NUMERIC

        # Support to Nullable wrapper
        if normalized_type.startswith("nullable("):
            inner_type = normalized_type[9:-1]
            return self._transform_column_type(inner_type)

        # Use the module-level mapping table
        mapped_type = CLICKHOUSE_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown ClickHouse data type: {data_type}")

        return mapped_type
