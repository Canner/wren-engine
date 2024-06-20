from json import loads

from app.model import PostgresConnectionInfo
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


class PostgresMetadata(Metadata):
    def __init__(self, connection_info: PostgresConnectionInfo):
        super().__init__(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                t.table_catalog,
                t.table_schema,
                t.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.ordinal_position
            FROM
                information_schema.tables t
            JOIN
                information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE
                t.table_type in ('BASE TABLE', 'VIEW')
                and t.table_schema not in ('information_schema', 'pg_catalog')
            """
        response = loads(
            DataSource.postgres.get_connection(self.connection_info)
            .sql(sql)
            .to_pandas()
            .to_json(orient="records")
        )

        unique_tables = {}
        for row in response:
            # generate unique table name
            schema_table = self._format_postgres_compact_table_name(
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
                        catalog=row["table_catalog"],
                        table=row["table_name"],
                    ),
                    primaryKey="",
                )

            # table exists, and add column to the table
            unique_tables[schema_table].columns.append(
                Column(
                    name=row["column_name"],
                    type=self._transform_postgres_column_type(row["data_type"]),
                    notNull=row["is_nullable"].lower() == "no",
                    description="",
                    properties=None,
                )
            )
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        sql = """
            SELECT
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            """
        res = loads(
            DataSource.postgres.get_connection(self.connection_info)
            .sql(sql, dialect="trino")
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
                        row["foreign_table_name"],
                        row["foreign_column_name"],
                    ),
                    constraintTable=self._format_postgres_compact_table_name(
                        row["table_schema"], row["table_name"]
                    ),
                    constraintColumn=row["column_name"],
                    constraintedTable=self._format_postgres_compact_table_name(
                        row["foreign_table_schema"], row["foreign_table_name"]
                    ),
                    constraintedColumn=row["foreign_column_name"],
                    constraintType=ConstraintType.FOREIGN_KEY,
                )
            )
        return constraints

    def _format_postgres_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _format_constraint_name(
        self, table_name, column_name, foreign_table_name, foreign_column_name
    ):
        return f"{table_name}_{column_name}_{foreign_table_name}_{foreign_column_name}"

    def _transform_postgres_column_type(self, data_type):
        # lower case the data_type
        data_type = data_type.lower()

        # all possible types listed here: https://www.postgresql.org/docs/current/datatype.html#DATATYPE-TABLE

        switcher = {
            "text": WrenEngineColumnType.TEXT,
            "char": WrenEngineColumnType.CHAR,
            "character": WrenEngineColumnType.CHAR,
            "bpchar": WrenEngineColumnType.CHAR,
            "name": WrenEngineColumnType.CHAR,
            "character varying": WrenEngineColumnType.VARCHAR,
            "bigint": WrenEngineColumnType.BIGINT,
            "int": WrenEngineColumnType.INTEGER,
            "integer": WrenEngineColumnType.INTEGER,
            "smallint": WrenEngineColumnType.SMALLINT,
            "real": WrenEngineColumnType.REAL,
            "double precision": WrenEngineColumnType.DOUBLE,
            "numeric": WrenEngineColumnType.DECIMAL,
            "decimal": WrenEngineColumnType.DECIMAL,
            "boolean": WrenEngineColumnType.BOOLEAN,
            "timestamp": WrenEngineColumnType.TIMESTAMP,
            "timestamp without time zone": WrenEngineColumnType.TIMESTAMP,
            "timestamp with time zone": WrenEngineColumnType.TIMESTAMPTZ,
            "date": WrenEngineColumnType.DATE,
            "interval": WrenEngineColumnType.INTERVAL,
            "json": WrenEngineColumnType.JSON,
            "bytea": WrenEngineColumnType.BYTEA,
            "uuid": WrenEngineColumnType.UUID,
            "inet": WrenEngineColumnType.INET,
            "oid": WrenEngineColumnType.OID,
        }

        return switcher.get(data_type, WrenEngineColumnType.UNKNOWN)


def to_json(df):
    json_obj = loads(df.to_json(orient="split"))
    del json_obj["index"]
    return json_obj
