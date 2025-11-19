from loguru import logger

from app.model import DatabricksTokenConnectionInfo
from app.model.connector import DatabricksConnector
from app.model.metadata.dto import (
    Column,
    Constraint,
    ConstraintType,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

# https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes
DATABRICKS_TYPE_MAPPING = {
    "bigint": RustWrenEngineColumnType.BIGINT,
    "binary": RustWrenEngineColumnType.BYTEA,
    "boolean": RustWrenEngineColumnType.BOOL,
    "date": RustWrenEngineColumnType.DATE,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "double": RustWrenEngineColumnType.DOUBLE,
    "float": RustWrenEngineColumnType.FLOAT,
    "int": RustWrenEngineColumnType.INTEGER,
    "smallint": RustWrenEngineColumnType.SMALLINT,
    "string": RustWrenEngineColumnType.STRING,
    "timestamp": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp_ntz": RustWrenEngineColumnType.TIMESTAMP,
    "tinyint": RustWrenEngineColumnType.TINYINT,
    "variant": RustWrenEngineColumnType.VARIANT,
    "object": RustWrenEngineColumnType.JSON,
}


class DatabricksMetadata(Metadata):
    def __init__(self, connection_info: DatabricksTokenConnectionInfo):
        super().__init__(connection_info)
        self.connection = DatabricksConnector(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                c.TABLE_CATALOG AS TABLE_CATALOG,
                c.TABLE_SCHEMA AS TABLE_SCHEMA,
                c.TABLE_NAME AS TABLE_NAME,
                c.COLUMN_NAME AS COLUMN_NAME,
                c.DATA_TYPE AS DATA_TYPE,
                c.IS_NULLABLE AS IS_NULLABLE,
                c.COMMENT AS COLUMN_COMMENT,
                t.COMMENT AS TABLE_COMMENT
            FROM
                INFORMATION_SCHEMA.COLUMNS c
            JOIN
                INFORMATION_SCHEMA.TABLES t
                ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
                AND c.TABLE_NAME = t.TABLE_NAME
                AND c.TABLE_CATALOG = t.TABLE_CATALOG
            WHERE
                c.TABLE_SCHEMA NOT IN ('information_schema')
        """
        response = self.connection.query(sql).to_pandas().to_dict(orient="records")

        unique_tables = {}
        for row in response:
            # generate unique table name
            schema_table = self._format_compact_table_name(
                row["TABLE_CATALOG"], row["TABLE_SCHEMA"], row["TABLE_NAME"]
            )
            # init table if not exists
            if schema_table not in unique_tables:
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description=row["TABLE_COMMENT"],
                    columns=[],
                    properties=TableProperties(
                        schema=row["TABLE_SCHEMA"],
                        catalog=row["TABLE_CATALOG"],
                        table=row["TABLE_NAME"],
                    ),
                    primaryKey="",
                )

            # table exists, and add column to the table
            data_type = row["DATA_TYPE"].lower()
            if data_type.startswith(("array", "map", "struct")):
                col_type = data_type
            else:
                col_type = self._transform_column_type(row["DATA_TYPE"])

            unique_tables[schema_table].columns.append(
                Column(
                    name=row["COLUMN_NAME"],
                    type=col_type,
                    notNull=row["IS_NULLABLE"].lower() == "no",
                    description=row["COLUMN_COMMENT"],
                    properties=None,
                )
            )
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        sql = """
            SELECT
                tc.table_catalog,
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                ccu.table_catalog AS foreign_table_catalog,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.constraint_schema = kcu.constraint_schema
                AND tc.table_catalog = kcu.table_catalog
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.constraint_catalog = tc.constraint_catalog
                AND ccu.constraint_schema = tc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            """
        res = self.connection.query(sql).to_pandas().to_dict(orient="records")
        constraints = []
        for row in res:
            constraints.append(
                Constraint(
                    constraintName=self._format_constraint_name(
                        row["table_name"],
                        row["column_name"],
                        row["foreign_table_name"],
                        row["foreign_column_name"],
                    ),
                    constraintTable=self._format_compact_table_name(
                        row["table_catalog"], row["table_schema"], row["table_name"]
                    ),
                    constraintColumn=row["column_name"],
                    constraintedTable=self._format_compact_table_name(
                        row["foreign_table_catalog"],
                        row["foreign_table_schema"],
                        row["foreign_table_name"],
                    ),
                    constraintedColumn=row["foreign_column_name"],
                    constraintType=ConstraintType.FOREIGN_KEY,
                )
            )
        return constraints

    def get_version(self) -> str:
        return (
            self.connection.query("SELECT current_version().dbsql_version")
            .to_pandas()
            .iloc[0, 0]
        )

    def _format_constraint_name(
        self, table_name, column_name, foreign_table_name, foreign_column_name
    ):
        return f"{table_name}_{column_name}_{foreign_table_name}_{foreign_column_name}"

    def _format_compact_table_name(self, catalog: str, schema: str, table: str):
        return f"{catalog}.{schema}.{table}"

    def _transform_column_type(self, data_type: str) -> RustWrenEngineColumnType:
        # Convert to lowercase for comparison
        normalized_type = data_type.lower()

        if normalized_type.startswith("decimal"):
            return RustWrenEngineColumnType.DECIMAL

        if normalized_type.startswith("geography"):
            return RustWrenEngineColumnType.GEOGRAPHY

        if normalized_type.startswith("geometry"):
            return RustWrenEngineColumnType.GEOMETRY

        # Use the module-level mapping table
        mapped_type = DATABRICKS_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown Databricks data type: {data_type}")

        return mapped_type
