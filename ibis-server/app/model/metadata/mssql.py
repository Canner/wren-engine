from json import loads

from app.model import MSSqlConnectionInfo
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


class MSSQLMetadata(Metadata):
    def __init__(self, connection_info: MSSqlConnectionInfo):
        super().__init__(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT 
                col.TABLE_CATALOG AS catalog,
                col.TABLE_SCHEMA AS table_schema,
                col.TABLE_NAME AS table_name,
                col.COLUMN_NAME AS column_name,
                col.DATA_TYPE AS data_type,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_pk,
                col.IS_NULLABLE AS is_nullable
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
            """
        response = loads(
            DataSource.mssql.get_connection(self.connection_info)
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
                    description="",
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
        res = loads(
            DataSource.mssql.get_connection(self.connection_info)
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
        # Define the mapping of MSSQL data types to WrenEngineColumnType
        # ref: https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15#exact-numerics
        switcher = {
            # String Types
            "char": WrenEngineColumnType.CHAR,
            "varchar": WrenEngineColumnType.VARCHAR,
            "text": WrenEngineColumnType.TEXT,
            "nchar": WrenEngineColumnType.CHAR,
            "nvarchar": WrenEngineColumnType.VARCHAR,
            "ntext": WrenEngineColumnType.TEXT,
            # Numeric Types
            "bit": WrenEngineColumnType.TINYINT,
            "tinyint": WrenEngineColumnType.TINYINT,
            "smallint": WrenEngineColumnType.SMALLINT,
            "int": WrenEngineColumnType.INTEGER,
            "bigint": WrenEngineColumnType.BIGINT,
            # Boolean
            "boolean": WrenEngineColumnType.BOOLEAN,
            # Decimal
            "float": WrenEngineColumnType.FLOAT8,
            "real": WrenEngineColumnType.FLOAT8,
            "decimal": WrenEngineColumnType.DECIMAL,
            "numeric": WrenEngineColumnType.NUMERIC,
            "money": WrenEngineColumnType.DECIMAL,
            "smallmoney": WrenEngineColumnType.DECIMAL,
            # Date and Time Types
            "date": WrenEngineColumnType.DATE,
            "datetime": WrenEngineColumnType.TIMESTAMP,
            "datetime2": WrenEngineColumnType.TIMESTAMPTZ,
            "smalldatetime": WrenEngineColumnType.TIMESTAMP,
            "time": WrenEngineColumnType.INTERVAL,
            "datetimeoffset": WrenEngineColumnType.TIMESTAMPTZ,
            # JSON Type (Note: MSSQL supports JSON natively as a string type)
            "json": WrenEngineColumnType.JSON,
        }

        return switcher.get(data_type.lower(), WrenEngineColumnType.UNKNOWN)


def to_json(df):
    json_obj = loads(df.to_json(orient="split"))
    del json_obj["index"]
    return json_obj
