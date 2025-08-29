from loguru import logger

from app.model import PostgresConnectionInfo
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

# PostgreSQL-specific type mappings
# All possible types listed here: https://www.postgresql.org/docs/current/datatype.html#DATATYPE-TABLE
POSTGRES_TYPE_MAPPING = {
    "text": RustWrenEngineColumnType.TEXT,
    "char": RustWrenEngineColumnType.CHAR,
    "character": RustWrenEngineColumnType.CHAR,
    "bpchar": RustWrenEngineColumnType.CHAR,
    "name": RustWrenEngineColumnType.CHAR,
    "character varying": RustWrenEngineColumnType.VARCHAR,
    "varchar": RustWrenEngineColumnType.VARCHAR,
    "bigint": RustWrenEngineColumnType.BIGINT,
    "int": RustWrenEngineColumnType.INTEGER,
    "int4": RustWrenEngineColumnType.INTEGER,
    "integer": RustWrenEngineColumnType.INTEGER,
    "smallint": RustWrenEngineColumnType.SMALLINT,
    "int2": RustWrenEngineColumnType.SMALLINT,
    "real": RustWrenEngineColumnType.REAL,
    "float4": RustWrenEngineColumnType.REAL,
    "double precision": RustWrenEngineColumnType.DOUBLE,
    "float8": RustWrenEngineColumnType.DOUBLE,
    "numeric": RustWrenEngineColumnType.DECIMAL,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "boolean": RustWrenEngineColumnType.BOOL,
    "bool": RustWrenEngineColumnType.BOOL,
    "timestamp": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp without time zone": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp with time zone": RustWrenEngineColumnType.TIMESTAMPTZ,
    "timestamptz": RustWrenEngineColumnType.TIMESTAMPTZ,
    "date": RustWrenEngineColumnType.DATE,
    "time": RustWrenEngineColumnType.TIME,
    "interval": RustWrenEngineColumnType.INTERVAL,
    "json": RustWrenEngineColumnType.JSON,
    "jsonb": RustWrenEngineColumnType.JSON,
    "bytea": RustWrenEngineColumnType.BYTEA,
    "uuid": RustWrenEngineColumnType.UUID,
    "inet": RustWrenEngineColumnType.INET,
    "oid": RustWrenEngineColumnType.OID,
}

# PostgreSQL extension type mappings
POSTGRES_EXTENSION_TYPE_MAPPING = {
    "geometry": RustWrenEngineColumnType.GEOMETRY,
    "geography": RustWrenEngineColumnType.GEOGRAPHY,
}


class ExtensionHandler:
    def __init__(self, connection):
        self.connection = connection

        self.handlers = {
            "postgis": self.postgis_handler,
        }

    def augment(self, tables: list[Table]) -> list[Table]:
        # Get the list of extensions from the database
        extensions = self.get_extensions()

        # Iterate through the extensions and call the appropriate handler
        for ext in extensions:
            ext_name = ext["extension_name"]
            schema_name = ext["schema_name"]

            if ext_name in self.handlers:
                handler = self.handlers[ext_name]
                tables = handler(tables, schema_name)

        return tables

    def get_extensions(self) -> list[str]:
        sql = """
            SELECT
                e.extname AS extension_name,
                n.nspname AS schema_name
            FROM
                pg_extension e
            JOIN
                pg_namespace n ON n.oid = e.extnamespace;
            """
        df = self.connection.sql(sql).to_pandas()
        if df.empty:
            return []
        response = df.to_dict(orient="records")
        return response

    def postgis_handler(self, tables: list[Table], schema_name: str) -> list[Table]:
        # Get the list of geometry and geography columns
        sql = f"""
            SELECT
                f_table_schema,
                f_table_name,
                f_geometry_column AS column_name,
                'geometry' AS column_type
            FROM
                {schema_name}.geometry_columns
            UNION ALL
            SELECT
                f_table_schema,
                f_table_name,
                f_geography_column AS column_name,
                'geography' AS column_type
            FROM
                {schema_name}.geography_columns;
            """
        response = self.connection.sql(sql).to_pandas().to_dict(orient="records")

        # Update tables
        for row in response:
            # TODO: Might want to use a global `_format_postgres_compact_table_name` function.
            table_name = f"{row['f_table_schema']}.{row['f_table_name']}"
            table = tables[table_name]
            for column in table.columns:
                column.type = str(
                    self._transform_postgres_column_type(row["column_type"])
                )
                break

        return tables

    def _transform_postgres_column_type(
        self, data_type: str
    ) -> RustWrenEngineColumnType:
        """Transform PostgreSQL extension column type.

        Args:
            data_type: The PostgreSQL extension data type string

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        # Convert to lowercase for comparison
        normalized_type = data_type.lower()

        # Use the module-level extension mapping table
        return POSTGRES_EXTENSION_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )


class PostgresMetadata(Metadata):
    def __init__(self, connection_info: PostgresConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.postgres.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                t.table_catalog,
                t.table_schema,
                t.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.ordinal_position,
                obj_description(cls.oid) AS table_comment,
                col_description(cls.oid, a.attnum) AS column_comment
            FROM
                information_schema.tables t
            JOIN
                information_schema.columns c
                ON t.table_schema = c.table_schema
                AND t.table_name = c.table_name
            LEFT JOIN
                pg_class cls
                ON cls.relname = t.table_name
                AND cls.relnamespace = (
                    SELECT oid FROM pg_namespace WHERE nspname = t.table_schema
                )
            LEFT JOIN
                pg_attribute a
                ON a.attrelid = cls.oid
                AND a.attname = c.column_name
            WHERE
                t.table_type IN ('BASE TABLE', 'VIEW')
                AND t.table_schema NOT IN ('information_schema', 'pg_catalog');
            """
        response = self.connection.sql(sql).to_pandas().to_dict(orient="records")

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
                    description=row["table_comment"],
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
                    description=row["column_comment"],
                    properties=None,
                )
            )
        extension_handler = ExtensionHandler(self.connection)
        unique_tables = extension_handler.augment(unique_tables)
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
        res = self.connection.sql(sql).to_pandas().to_dict(orient="records")
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

    def get_version(self) -> str:
        return self.connection.sql("SELECT version()").to_pandas().iloc[0, 0]

    def _format_postgres_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _format_constraint_name(
        self, table_name, column_name, foreign_table_name, foreign_column_name
    ):
        return f"{table_name}_{column_name}_{foreign_table_name}_{foreign_column_name}"

    def _transform_postgres_column_type(
        self, data_type: str
    ) -> RustWrenEngineColumnType:
        """Transform PostgreSQL data type to RustWrenEngineColumnType.

        Args:
            data_type: The PostgreSQL data type string

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        # Convert to lowercase for comparison
        normalized_type = data_type.lower()

        # Use the module-level mapping table
        mapped_type = POSTGRES_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown Postgres data type: {data_type}")

        return mapped_type
