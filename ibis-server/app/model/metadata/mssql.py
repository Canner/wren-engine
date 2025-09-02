from loguru import logger

from app.model import MSSqlConnectionInfo
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

# MSSQL-specific type mapping
# Reference: https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#exact-numerics
MSSQL_TYPE_MAPPING = {
    # String Types
    "char": RustWrenEngineColumnType.CHAR,
    "varchar": RustWrenEngineColumnType.VARCHAR,
    "text": RustWrenEngineColumnType.TEXT,
    "nchar": RustWrenEngineColumnType.CHAR,
    "nvarchar": RustWrenEngineColumnType.VARCHAR,
    "ntext": RustWrenEngineColumnType.TEXT,
    # Numeric Types
    "bit": RustWrenEngineColumnType.TINYINT,
    "tinyint": RustWrenEngineColumnType.TINYINT,
    "smallint": RustWrenEngineColumnType.SMALLINT,
    "int": RustWrenEngineColumnType.INTEGER,
    "bigint": RustWrenEngineColumnType.BIGINT,
    # Boolean
    "boolean": RustWrenEngineColumnType.BOOL,
    # Decimal
    "float": RustWrenEngineColumnType.FLOAT8,
    "real": RustWrenEngineColumnType.FLOAT8,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "numeric": RustWrenEngineColumnType.NUMERIC,
    "money": RustWrenEngineColumnType.DECIMAL,
    "smallmoney": RustWrenEngineColumnType.DECIMAL,
    # Date and Time Types
    "date": RustWrenEngineColumnType.DATE,
    "datetime": RustWrenEngineColumnType.TIMESTAMP,
    "datetime2": RustWrenEngineColumnType.TIMESTAMP,
    "smalldatetime": RustWrenEngineColumnType.TIMESTAMP,
    "time": RustWrenEngineColumnType.INTERVAL,
    "datetimeoffset": RustWrenEngineColumnType.TIMESTAMPTZ,
    # JSON Type (Note: MSSQL supports JSON natively as a string type)
    "json": RustWrenEngineColumnType.JSON,
}


class MSSQLMetadata(Metadata):
    def __init__(self, connection_info: MSSqlConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.mssql.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT 
                col.TABLE_CATALOG AS catalog,
                col.TABLE_SCHEMA AS table_schema,
                col.TABLE_NAME AS table_name,
                col.COLUMN_NAME AS column_name,
                col.DATA_TYPE AS data_type,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_pk,
                col.IS_NULLABLE AS is_nullable,
                CAST(tprop.value AS NVARCHAR(MAX)) AS table_comment,
                CAST(cprop.value AS NVARCHAR(MAX)) AS column_comment
            FROM 
                INFORMATION_SCHEMA.COLUMNS col
            LEFT JOIN 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tab
                ON col.TABLE_SCHEMA = tab.TABLE_SCHEMA
                AND col.TABLE_NAME = tab.TABLE_NAME
                AND tab.CONSTRAINT_TYPE = 'PRIMARY KEY'
            LEFT JOIN 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
                ON col.TABLE_SCHEMA = pk.TABLE_SCHEMA
                AND col.TABLE_NAME = pk.TABLE_NAME
                AND col.COLUMN_NAME = pk.COLUMN_NAME
                AND pk.CONSTRAINT_NAME = tab.CONSTRAINT_NAME
            LEFT JOIN 
                sys.tables st
                ON st.name = col.TABLE_NAME 
                AND SCHEMA_NAME(st.schema_id) = col.TABLE_SCHEMA
            LEFT JOIN 
                sys.extended_properties tprop
                ON tprop.major_id = st.object_id 
                AND tprop.minor_id = 0 
                AND tprop.name = 'MS_Description'
            LEFT JOIN 
                sys.columns sc
                ON sc.object_id = st.object_id 
                AND sc.name = col.COLUMN_NAME
            LEFT JOIN 
                sys.extended_properties cprop
                ON cprop.major_id = sc.object_id 
                AND cprop.minor_id = sc.column_id 
                AND cprop.name = 'MS_Description'
            WHERE
                col.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA');
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
                        catalog=row["catalog"],
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
            if row["is_pk"] == "YES":
                unique_tables[schema_table].primaryKey = row["column_name"]
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        # The REFERENTIAL_CONSTRAINTS table provides information about foreign keys.
        # The KEY_COLUMN_USAGE table describes which key columns have constraints.
        sql = """
            SELECT 
                fk.name AS constraint_name,
                sch.name AS table_schema,
                t.name AS table_name,
                c.name AS column_name,
                ref_sch.name AS referenced_table_schema,
                ref_t.name AS referenced_table_name,
                ref_c.name AS referenced_column_name
            FROM 
                sys.foreign_keys AS fk
            JOIN 
                sys.foreign_key_columns AS fkc 
                ON fk.object_id = fkc.constraint_object_id
            JOIN 
                sys.tables AS t 
                ON fkc.parent_object_id = t.object_id
            JOIN 
                sys.schemas AS sch 
                ON t.schema_id = sch.schema_id
            JOIN 
                sys.columns AS c 
                ON fkc.parent_column_id = c.column_id 
                AND c.object_id = t.object_id
            JOIN 
                sys.tables AS ref_t 
                ON fkc.referenced_object_id = ref_t.object_id
            JOIN 
                sys.schemas AS ref_sch 
                ON ref_t.schema_id = ref_sch.schema_id
            JOIN 
                sys.columns AS ref_c 
                ON fkc.referenced_column_id = ref_c.column_id 
                AND ref_c.object_id = ref_t.object_id
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
        return self.connection.sql("SELECT @@VERSION").to_pandas().iloc[0, 0]

    def _format_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _format_constraint_name(
        self, table_name, column_name, referenced_table_name, referenced_column_name
    ):
        return f"{table_name}_{column_name}_{referenced_table_name}_{referenced_column_name}"

    def _transform_column_type(self, data_type: str) -> RustWrenEngineColumnType:
        """Transform MSSQL data type to RustWrenEngineColumnType.

        Args:
            data_type: The MSSQL data type string

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        # Convert to lowercase for comparison
        normalized_type = data_type.lower()

        # Use the module-level mapping table
        mapped_type = MSSQL_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown MSSQL data type: {data_type}")

        return mapped_type
