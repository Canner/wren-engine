import re

from loguru import logger

from app.model import DorisConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Catalog,
    Column,
    Constraint,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

# Doris-specific type mapping
# Doris is MySQL-protocol compatible but has additional types
# Reference: https://doris.apache.org/docs/sql-manual/data-types/
DORIS_TYPE_MAPPING = {
    # ── String Types ─────────────────────────────
    "char": RustWrenEngineColumnType.CHAR,
    "varchar": RustWrenEngineColumnType.VARCHAR,
    "string": RustWrenEngineColumnType.VARCHAR,
    "text": RustWrenEngineColumnType.TEXT,
    "tinytext": RustWrenEngineColumnType.TEXT,
    "mediumtext": RustWrenEngineColumnType.TEXT,
    "longtext": RustWrenEngineColumnType.TEXT,
    # ── Numeric Types ────────────────────────────
    "tinyint": RustWrenEngineColumnType.TINYINT,
    "smallint": RustWrenEngineColumnType.SMALLINT,
    "int": RustWrenEngineColumnType.INTEGER,
    "integer": RustWrenEngineColumnType.INTEGER,
    "mediumint": RustWrenEngineColumnType.INTEGER,
    "bigint": RustWrenEngineColumnType.BIGINT,
    "largeint": RustWrenEngineColumnType.BIGINT,
    # ── Boolean Types ────────────────────────────
    "boolean": RustWrenEngineColumnType.BOOL,
    "bool": RustWrenEngineColumnType.BOOL,
    # ── Decimal Types ────────────────────────────
    "float": RustWrenEngineColumnType.FLOAT8,
    "double": RustWrenEngineColumnType.DOUBLE,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "decimalv3": RustWrenEngineColumnType.DECIMAL,
    "numeric": RustWrenEngineColumnType.NUMERIC,
    # ── Date and Time Types ──────────────────────
    "date": RustWrenEngineColumnType.DATE,
    "datev2": RustWrenEngineColumnType.DATE,
    "datetime": RustWrenEngineColumnType.TIMESTAMP,
    "datetimev2": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp": RustWrenEngineColumnType.TIMESTAMPTZ,
    # ── JSON Type ────────────────────────────────
    "json": RustWrenEngineColumnType.JSON,
    "jsonb": RustWrenEngineColumnType.JSON,
    "variant": RustWrenEngineColumnType.JSON,
    # ── Complex Types (map to JSON for compatibility) ─
    "array": RustWrenEngineColumnType.JSON,
    "map": RustWrenEngineColumnType.JSON,
    "struct": RustWrenEngineColumnType.JSON,
    # ── Doris-specific aggregate types (map to VARCHAR) ─
    "hll": RustWrenEngineColumnType.VARCHAR,
    "bitmap": RustWrenEngineColumnType.VARCHAR,
    "quantile_state": RustWrenEngineColumnType.VARCHAR,
    "agg_state": RustWrenEngineColumnType.VARCHAR,
}


class DorisMetadata(Metadata):
    def __init__(self, connection_info: DorisConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.doris.get_connection(connection_info)
        self.database = connection_info.database.get_secret_value()

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                c.TABLE_SCHEMA AS table_schema,
                c.TABLE_NAME AS table_name,
                c.COLUMN_NAME AS column_name,
                c.COLUMN_TYPE AS data_type,
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
                c.TABLE_SCHEMA NOT IN ('information_schema', '__internal_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY
                c.TABLE_SCHEMA,
                c.TABLE_NAME,
                c.ORDINAL_POSITION;
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
                        catalog="",
                        table=row["table_name"],
                    ),
                    primaryKey=None,
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
            # if column is primary key (Doris Unique Key model)
            if row["column_key"] == "UNI" or row["column_key"] == "PRI":
                existing_pk = unique_tables[schema_table].primaryKey
                if existing_pk:
                    # Support composite keys
                    unique_tables[
                        schema_table
                    ].primaryKey = f"{existing_pk},{row['column_name']}"
                else:
                    unique_tables[schema_table].primaryKey = row["column_name"]
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        # Doris does not support foreign key constraints.
        # Return an empty list as there are no referential constraints.
        return []

    def get_schema_list(self, filter_info=None, limit=None) -> list[Catalog]:
        sql = """
            SELECT SCHEMA_NAME
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('information_schema', '__internal_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY SCHEMA_NAME
        """
        if limit is not None:
            try:
                validated_limit = int(limit)
            except (TypeError, ValueError) as exc:
                raise ValueError("limit must be an integer") from exc
            if validated_limit < 0:
                raise ValueError("limit must be non-negative")
            sql += f" LIMIT {validated_limit}"
        response = self.connection.sql(sql).to_pandas()
        schemas = response["SCHEMA_NAME"].tolist()
        # Doris has a flat namespace (no multi-catalog).
        # Return a single Catalog entry whose name is the catalog reported by
        # the current connection (usually "internal"), with all user databases
        # listed as schemas.
        try:
            catalog_name = (
                self.connection.sql("SELECT CURRENT_CATALOG()").to_pandas().iloc[0, 0]
            )
        except Exception:
            catalog_name = "internal"
        return [Catalog(name=catalog_name, schemas=schemas)]

    def get_version(self) -> str:
        return self.connection.sql("SELECT version()").to_pandas().iloc[0, 0]

    def _format_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _format_constraint_name(
        self, table_name, column_name, referenced_table_name, referenced_column_name
    ):
        return f"{table_name}_{column_name}_{referenced_table_name}_{referenced_column_name}"

    def _transform_column_type(self, data_type: str) -> RustWrenEngineColumnType:
        """Transform Doris data type to RustWrenEngineColumnType.

        Uses COLUMN_TYPE from information_schema which returns Doris-native
        type names (e.g. 'largeint', 'string', 'decimalv3(10,2)') rather
        than MySQL-compatible DATA_TYPE (which maps largeint → 'bigint unsigned').

        Args:
            data_type: The Doris COLUMN_TYPE string

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        # Strip precision/length info: int(11) -> int, decimalv3(10,2) -> decimalv3
        # Also strip angle-bracket generics: ARRAY<INT> -> array, MAP<STRING,INT> -> map
        normalized_type = re.sub(r"(<.*>|\(.*\))", "", data_type.strip()).lower()

        # Use the module-level mapping table
        mapped_type = DORIS_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown Doris data type: {data_type}")

        return mapped_type
