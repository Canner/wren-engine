import ibis

from app.model import OracleConnectionInfo
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


class OracleMetadata(Metadata):
    def __init__(self, connection_info: OracleConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.oracle.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                t.owner AS TABLE_CATALOG,
                t.owner AS TABLE_SCHEMA,
                t.table_name AS TABLE_NAME,
                c.column_name AS COLUMN_NAME,
                c.data_type AS DATA_TYPE,
                c.nullable AS IS_NULLABLE,
                c.column_id AS ORDINAL_POSITION,
                tc.comments AS TABLE_COMMENT,
                cc.comments AS COLUMN_COMMENT
            FROM
                all_tables t
            JOIN
                all_tab_columns c
                ON t.owner = c.owner
                AND t.table_name = c.table_name
            LEFT JOIN
                all_tab_comments tc
                ON tc.owner = t.owner
                AND tc.table_name = t.table_name
            LEFT JOIN
                all_col_comments cc
                ON cc.owner = c.owner
                AND cc.table_name = c.table_name
                AND cc.column_name = c.column_name
            WHERE
                t.owner = 'SYSTEM'
            ORDER BY
                t.table_name, c.column_id;
        """
        # Provide the schema explicitly with uppercase column names.
        schema = ibis.schema(
            {
                "TABLE_CATALOG": "string",
                "TABLE_SCHEMA": "string",
                "TABLE_NAME": "string",
                "COLUMN_NAME": "string",
                "DATA_TYPE": "string",
                "IS_NULLABLE": "string",
                "ORDINAL_POSITION": "int64",
                "TABLE_COMMENT": "string",
                "COLUMN_COMMENT": "string",
            }
        )
        response = (
            self.connection.sql(sql, schema=schema)
            .to_pandas()
            .to_dict(orient="records")
        )

        unique_tables = {}
        for row in response:
            # Use uppercase keys that match the provided schema.
            schema_table = self._format_compact_table_name(
                row["TABLE_SCHEMA"], row["TABLE_NAME"]
            )
            if schema_table not in unique_tables:
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description=row["TABLE_COMMENT"],
                    columns=[],
                    properties=TableProperties(
                        schema=row["TABLE_SCHEMA"],
                        catalog="",  # Oracle doesn't use catalogs.
                        table=row["TABLE_NAME"],
                    ),
                    primaryKey="",
                )

            unique_tables[schema_table].columns.append(
                Column(
                    name=row["COLUMN_NAME"],
                    type=self._transform_column_type(row["DATA_TYPE"]),
                    notNull=row["IS_NULLABLE"] == "N",
                    description=row["COLUMN_COMMENT"],
                    properties=None,
                )
            )
            # If the row indicates a primary key, set it.
            if row.get("IS_PK", "NO") == "YES":
                unique_tables[schema_table].primaryKey = row["COLUMN_NAME"]

        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        sql = """
            SELECT 
                a.owner as table_schema,
                a.table_name as table_name,
                a.column_name as column_name,
                a_pk.owner as referenced_table_schema,
                a_pk.table_name as referenced_table_name,
                a_pk.column_name as referenced_column_name
            FROM 
                dba_cons_columns a
            JOIN 
                dba_constraints c 
                ON a.owner = c.owner
                AND a.constraint_name = c.constraint_name
            JOIN 
                dba_constraints c_pk 
                ON c.r_owner = c_pk.owner
                AND c.r_constraint_name = c_pk.constraint_name
            JOIN 
                dba_cons_columns a_pk
                ON c_pk.owner = a_pk.owner
                AND c_pk.constraint_name = a_pk.constraint_name
            WHERE 
                c.constraint_type = 'R'
            ORDER BY 
                a.owner,
                a.table_name,
                a.column_name
        """
        # For this query, we rely on the SQL aliases as defined (lowercase)
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
        return (
            self.connection.sql("SELECT version FROM v$instance").to_pandas().iloc[0, 0]
        )

    def _format_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _format_constraint_name(
        self, table_name, column_name, referenced_table_name, referenced_column_name
    ):
        return f"{table_name}_{column_name}_{referenced_table_name}_{referenced_column_name}"

    def _transform_column_type(self, data_type):
        switcher = {
            "CHAR": RustWrenEngineColumnType.CHAR,
            "NCHAR": RustWrenEngineColumnType.CHAR,
            "VARCHAR2": RustWrenEngineColumnType.VARCHAR,
            "NVARCHAR2": RustWrenEngineColumnType.VARCHAR,
            "CLOB": RustWrenEngineColumnType.TEXT,
            "NCLOB": RustWrenEngineColumnType.TEXT,
            "NUMBER": RustWrenEngineColumnType.DECIMAL,
            "FLOAT": RustWrenEngineColumnType.FLOAT8,
            "BINARY_FLOAT": RustWrenEngineColumnType.FLOAT8,
            "BINARY_DOUBLE": RustWrenEngineColumnType.DOUBLE,
            "DATE": RustWrenEngineColumnType.TIMESTAMP,  # Oracle DATE includes time.
            "TIMESTAMP": RustWrenEngineColumnType.TIMESTAMP,
            "TIMESTAMP WITH TIME ZONE": RustWrenEngineColumnType.TIMESTAMPTZ,
            "TIMESTAMP WITH LOCAL TIME ZONE": RustWrenEngineColumnType.TIMESTAMPTZ,
            "INTERVAL YEAR TO MONTH": RustWrenEngineColumnType.INTERVAL,
            "INTERVAL DAY TO SECOND": RustWrenEngineColumnType.INTERVAL,
            "BLOB": RustWrenEngineColumnType.BYTEA,
            "BFILE": RustWrenEngineColumnType.BYTEA,
            "RAW": RustWrenEngineColumnType.BYTEA,
            "LONG RAW": RustWrenEngineColumnType.BYTEA,
            "ROWID": RustWrenEngineColumnType.CHAR,
            "UROWID": RustWrenEngineColumnType.CHAR,
            "JSON": RustWrenEngineColumnType.JSON,
            "OSON": RustWrenEngineColumnType.JSON,
            "VARCHAR2 WITH JSON": RustWrenEngineColumnType.JSON,
            "BLOB WITH JSON": RustWrenEngineColumnType.JSON,
            "CLOB WITH JSON": RustWrenEngineColumnType.JSON,
        }
        return switcher.get(data_type.upper(), RustWrenEngineColumnType.UNKNOWN)
