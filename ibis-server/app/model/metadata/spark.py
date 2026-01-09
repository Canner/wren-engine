from loguru import logger

from app.model import SparkConnectionInfo
from app.model.connector import Connector
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
        self.connector = Connector(DataSource.spark, connection_info)

    def get_table_list(self) -> list[Table]:
        # Discover tables across all catalogs.
        # catalog -> schema -> table

        tables: dict[str, Table] = {}

        for catalog in self._list_catalogs():
            for schema in self._list_schemas(catalog):
                if schema in ("information_schema", "sys"):
                    continue

                try:
                    tables_df = self._list_tables(catalog, schema)
                except Exception as e:
                    logger.warning(
                        f"Failed to list tables from {catalog}.{schema}: {e}"
                    )
                    continue

                for _, row in tables_df.iterrows():
                    table_name = row.get("tableName") or row.get("table")
                    if not table_name:
                        continue

                    compact_name = self._format_spark_compact_table_name(
                        catalog, schema, table_name
                    )

                    try:
                        columns = self._describe_table(catalog, schema, table_name)
                    except Exception as e:
                        logger.warning(
                            f"Failed to describe table {catalog}.{schema}.{table_name}: {e}"
                        )
                        continue

                    tables[compact_name] = Table(
                        name=compact_name,
                        description=None,
                        columns=columns,
                        properties=TableProperties(
                            catalog=catalog,
                            schema=schema,
                            table=table_name,
                        ),
                        primaryKey="",
                    )

        return list(tables.values())

    def get_constraints(self) -> list[Constraint]:
        """Get foreign key constraints across catalogs.

        Notes:
        - Constraints are informational only in Spark.
        - Only catalogs that implement `information_schema` will work.
        - `spark_catalog` typically does NOT support this.
        """
        constraints: list[Constraint] = []

        for catalog in self._list_catalogs():
            sql = f"""
                SELECT
                    fk.table_schema,
                    fk.table_name,
                    fk.column_name,
                    fk.constraint_name,
                    pk.table_schema AS foreign_table_schema,
                    pk.table_name AS foreign_table_name,
                    pk.column_name AS foreign_column_name
                FROM
                    `{catalog}`.information_schema.referential_constraints rc
                JOIN
                    `{catalog}`.information_schema.key_column_usage fk
                    ON rc.constraint_name = fk.constraint_name
                JOIN
                    `{catalog}`.information_schema.key_column_usage pk
                    ON rc.unique_constraint_name = pk.constraint_name
            """

            try:
                df = self.connector.query(sql).to_pandas()
            except Exception:
                # Expected for spark_catalog and many deployments
                continue

            for row in df.to_dict(orient="records"):
                constraints.append(
                    Constraint(
                        constraintName=self._format_constraint_name(
                            row["table_name"],
                            row["column_name"],
                            row["foreign_table_name"],
                            row["foreign_column_name"],
                        ),
                        constraintTable=self._format_spark_compact_table_name(
                            catalog,
                            row["table_schema"],
                            row["table_name"],
                        ),
                        constraintColumn=row["column_name"],
                        constraintedTable=self._format_spark_compact_table_name(
                            catalog,
                            row["foreign_table_schema"],
                            row["foreign_table_name"],
                        ),
                        constraintedColumn=row["foreign_column_name"],
                        constraintType=ConstraintType.FOREIGN_KEY,
                    )
                )

        return constraints

    def get_version(self) -> str:
        df = self.connector.query("SELECT version()")
        return df.column(0)[0].as_py()

    # ---------------------------------------------------------------------
    # Catalog / schema / table discovery helpers
    # ---------------------------------------------------------------------

    def _list_catalogs(self) -> list[str]:
        df = self.connector.query("SHOW CATALOGS").to_pandas()
        return df.iloc[:, 0].tolist()

    def _list_schemas(self, catalog: str) -> list[str]:
        df = self.connector.query(f"SHOW SCHEMAS FROM `{catalog}`").to_pandas()
        return df.iloc[:, 0].tolist()

    def _list_tables(self, catalog: str, schema: str):
        return self.connector.query(
            f"SHOW TABLES FROM `{catalog}`.`{schema}`"
        ).to_pandas()

    def _describe_table(self, catalog: str, schema: str, table: str) -> list[Column]:
        df = self.connector.query(
            f"DESCRIBE `{catalog}`.`{schema}`.`{table}`"
        ).to_pandas()

        columns: list[Column] = []

        for _, row in df.iterrows():
            col_name = str(row.get("col_name", "")).strip()

            # Stop at partition / metadata sections
            if not col_name or col_name.startswith("#"):
                break

            data_type = str(row.get("data_type", "")).strip()
            comment = row.get("comment")

            columns.append(
                Column(
                    name=col_name,
                    type=self._transform_spark_column_type(data_type),
                    notNull=False,
                    description=comment if comment else None,
                    properties=None,
                )
            )

        return columns

    def _format_spark_compact_table_name(
        self, catalog: str, schema: str, table: str
    ) -> str:
        return f"{catalog}.{schema}.{table}"

    def _format_constraint_name(
        self,
        table_name: str,
        column_name: str,
        foreign_table_name: str,
        foreign_column_name: str,
    ) -> str:
        return f"{table_name}_{column_name}_{foreign_table_name}_{foreign_column_name}"

    def _transform_spark_column_type(self, data_type: str) -> RustWrenEngineColumnType:
        normalized_type = data_type.lower().strip()
        base_type = normalized_type.split("(")[0].split("<")[0].strip()

        mapped_type = SPARK_TYPE_MAPPING.get(
            base_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown Spark data type: {data_type}")

        return mapped_type
