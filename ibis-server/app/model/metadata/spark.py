from loguru import logger

from app.model import SparkConnectionInfo
from app.model.connector import Connector
from app.model.metadata.dto import (
    Column,
    Constraint,
    ConstraintType,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

# Spark/Hive type mappings
# Reference: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
SPARK_TYPE_MAPPING = {
    "string": RustWrenEngineColumnType.VARCHAR,
    "varchar": RustWrenEngineColumnType.VARCHAR,
    "char": RustWrenEngineColumnType.CHAR,
    "bigint": RustWrenEngineColumnType.BIGINT,
    "long": RustWrenEngineColumnType.BIGINT,
    "int": RustWrenEngineColumnType.INTEGER,
    "integer": RustWrenEngineColumnType.INTEGER,
    "smallint": RustWrenEngineColumnType.SMALLINT,
    "short": RustWrenEngineColumnType.SMALLINT,
    "tinyint": RustWrenEngineColumnType.TINYINT,
    "byte": RustWrenEngineColumnType.TINYINT,
    "float": RustWrenEngineColumnType.REAL,
    "real": RustWrenEngineColumnType.REAL,
    "double": RustWrenEngineColumnType.DOUBLE,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "numeric": RustWrenEngineColumnType.DECIMAL,
    "boolean": RustWrenEngineColumnType.BOOL,
    "timestamp": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp_ntz": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp_ltz": RustWrenEngineColumnType.TIMESTAMPTZ,
    "date": RustWrenEngineColumnType.DATE,
    "binary": RustWrenEngineColumnType.BYTEA,
    "map": RustWrenEngineColumnType.JSON,
    "struct": RustWrenEngineColumnType.JSON,
    "interval": RustWrenEngineColumnType.INTERVAL,
}


class SparkMetadata(Metadata):
    def __init__(self, connection_info: SparkConnectionInfo):
        super().__init__(connection_info)
        self.connector = Connector("spark", connection_info)

    def get_table_list(self) -> list[Table]:
        tables_sql = "SHOW TABLES"
        tables_table = self.connector.query(tables_sql)
        tables_df = tables_table.to_pandas()

        unique_tables = {}

        for _, row in tables_df.iterrows():
            database = row["namespace"] if "namespace" in row else row["database"]
            table_name = row["tableName"]
            schema_table = self._format_spark_compact_table_name(database, table_name)

            # Skip system schemas
            if database in ("information_schema", "sys"):
                continue

            # Get columns
            try:
                columns_sql = f"DESCRIBE {database}.{table_name}"
                columns_table = self.connector.query(columns_sql)
                columns_df = columns_table.to_pandas()

                # Initialize table
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description=None,
                    columns=[],
                    properties=TableProperties(
                        schema=database,
                        catalog="spark_catalog",
                        table=table_name,
                    ),
                    primaryKey="",
                )

                # Add columns
                for _, col_row in columns_df.iterrows():
                    col_name = str(col_row["col_name"]).strip()

                    # Stop at partition or metadata sections
                    if col_name.startswith("#") or col_name == "":
                        break

                    data_type = str(col_row["data_type"]).strip()
                    col_comment = col_row.get("comment")

                    unique_tables[schema_table].columns.append(
                        Column(
                            name=col_name,
                            type=self._transform_spark_column_type(data_type),
                            notNull=False,
                            description=col_comment if col_comment else None,
                            properties=None,
                        )
                    )

            except Exception as e:
                logger.warning(f"Failed to describe table {database}.{table_name}: {e}")
                continue

        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        """Get foreign key constraints from Spark catalog.

        Note: Spark/Hive historically has limited support for constraints.
        Foreign keys are informational only and not enforced.
        This may return empty results for many Spark deployments.
        """
        # Spark 3.x supports informational foreign keys in some catalogs
        # This query may not work for all Spark deployments
        sql = """
            SELECT
                fk.table_schema,
                fk.table_name,
                fk.column_name,
                fk.constraint_name,
                pk.table_schema AS foreign_table_schema,
                pk.table_name AS foreign_table_name,
                pk.column_name AS foreign_column_name
            FROM
                information_schema.referential_constraints rc
            JOIN
                information_schema.key_column_usage fk
                ON rc.constraint_name = fk.constraint_name
                AND rc.constraint_schema = fk.table_schema
            JOIN
                information_schema.key_column_usage pk
                ON rc.unique_constraint_name = pk.constraint_name
                AND rc.unique_constraint_schema = pk.table_schema
            WHERE
                fk.table_schema NOT IN ('information_schema', 'sys')
            """

        try:
            df = self.connector.query(sql)
            res = df.to_dict(orient="records")
        except Exception as e:
            logger.warning(
                f"Failed to get constraints from Spark (this is normal for many Spark deployments): {e}"
            )
            return []

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
                    constraintTable=self._format_spark_compact_table_name(
                        row["table_schema"], row["table_name"]
                    ),
                    constraintColumn=row["column_name"],
                    constraintedTable=self._format_spark_compact_table_name(
                        row["foreign_table_schema"], row["foreign_table_name"]
                    ),
                    constraintedColumn=row["foreign_column_name"],
                    constraintType=ConstraintType.FOREIGN_KEY,
                )
            )
        return constraints

    def get_version(self) -> str:
        df = self.connector.query("SELECT version()")
        return df.column(0)[0].as_py()

    def _format_spark_compact_table_name(self, schema: str, table: str) -> str:
        """Format table name as schema.table."""
        return f"{schema}.{table}"

    def _format_constraint_name(
        self,
        table_name: str,
        column_name: str,
        foreign_table_name: str,
        foreign_column_name: str,
    ) -> str:
        """Generate constraint name from table and column names."""
        return f"{table_name}_{column_name}_{foreign_table_name}_{foreign_column_name}"

    def _transform_spark_column_type(self, data_type: str) -> RustWrenEngineColumnType:
        """Transform Spark data type to RustWrenEngineColumnType.

        Args:
            data_type: The Spark data type string (e.g., 'int', 'string', 'decimal(10,2)')

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        # Convert to lowercase for comparison
        normalized_type = data_type.lower().strip()

        # Handle parameterized types (e.g., decimal(10,2), varchar(100), array<int>)
        base_type = normalized_type.split("(")[0].split("<")[0].strip()

        # Use the module-level mapping table
        mapped_type = SPARK_TYPE_MAPPING.get(
            base_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown Spark data type: {data_type}")

        return mapped_type
