from app.model.data_source import DataSource
from app.model.dto import PostgresConnectionUrl, PostgresConnectionInfo
from enum import StrEnum, auto
from json import loads
from app.model.metadata_dto import (
    CompactTable,
    CompactColumn,
    Constraint,
    WrenEngineColumnType,
    ConstraintType,
)


class Metadata(StrEnum):
    postgres = auto()
    bigquery = auto()

    def get_table_list(self, connection_info):
        if self == Metadata.postgres:
            return self.get_postgres_table_list_sql(connection_info)
        if self == Metadata.bigquery:
            return self.get_bigquery_table_list_sql(connection_info)
        raise NotImplementedError(f"Unsupported data source: {self}")

    def get_constraints(self, connection_info):
        if self == Metadata.postgres:
            return self.get_postgres_table_constraints(connection_info)
        if self == Metadata.bigquery:
            return self.get_bigquery_table_constraints(connection_info)
        raise NotImplementedError(f"Unsupported data source: {self}")

    @staticmethod
    def get_postgres_table_list_sql(
        connection_info: PostgresConnectionUrl | PostgresConnectionInfo,
    ):
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
        res = to_json(
            DataSource.postgres.get_connection(connection_info)
            .sql(sql, dialect="trino")
            .to_pandas()
        )

        # transform the result to a list of dictionaries
        response = [
            (
                lambda x: {
                    "table_catalog": x[0],
                    "table_schema": x[1],
                    "table_name": x[2],
                    "column_name": x[3],
                    "data_type": transform_postgres_column_type(x[4]),
                    "is_nullable": x[5],
                    "ordinal_position": x[6],
                }
            )(row)
            for row in res["data"]
        ]

        unique_tables = {}
        for row in response:
            # generate unique table name
            schema_table = f"{row['table_schema']}.{row['table_name']}"
            # init table if not exists
            if schema_table not in unique_tables:
                table = {
                    "name": schema_table,
                    "description": "",
                    "columns": [],
                    "properties": {
                        "schema": row["table_schema"],
                        "catalog": row["table_catalog"],
                        "table": row["table_name"],
                    },
                    "primaryKey": "",
                }
                unique_tables[schema_table] = CompactTable(**table)
            # table exists, and add column to the table
            column = {
                "name": row["column_name"],
                "type": row["data_type"],
                "notNull": row["is_nullable"].lower() == "no",
                "description": "",
                "properties": {},
            }
            column = CompactColumn(**column)
            unique_tables[schema_table].columns.append(column)

        compact_tables: list[CompactTable] = list(unique_tables.values())
        return compact_tables

    @staticmethod
    def get_bigquery_table_list_sql(connection_info):
        dataset_id = connection_info.dataset_id
        sql = f"""
            SELECT 
                c.table_catalog,
                c.table_schema,
                c.table_name,
                c.column_name,
                c.ordinal_position,
                c.is_nullable,
                c.data_type,
                c.is_generated,
                c.generation_expression,
                c.is_stored,
                c.is_hidden,
                c.is_updatable,
                c.is_system_defined,
                c.is_partitioning_column,
                c.clustering_ordinal_position,
                c.collation_name,
                c.column_default,
                c.rounding_mode,
                cf.description AS column_description, 
                table_options.option_value AS table_description
            FROM {dataset_id}.INFORMATION_SCHEMA.COLUMNS c 
            JOIN {dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS cf 
                ON cf.table_name = c.table_name 
                AND cf.column_name = c.column_name
            LEFT JOIN {dataset_id}.INFORMATION_SCHEMA.TABLE_OPTIONS table_options
                ON c.table_name = table_options.table_name
            WHERE
                cf.column_name = cf.field_path
                AND NOT REGEXP_CONTAINS(cf.data_type, r'^(STRUCT|ARRAY<STRUCT)')
            """
        print(sql)
        res = to_json(
            DataSource.bigquery.get_connection(connection_info)
            .sql(sql, dialect="bigquery")
            .to_pandas()
        )

        # transform the result to a list of dictionaries
        res = [
            (
                lambda x: {
                    "table_catalog": x[0],
                    "table_schema": x[1],
                    "table_name": x[2],
                    "column_name": x[3],
                    "ordinal_position": x[4],
                    "is_nullable": x[5],
                    "data_type": x[6],
                    "is_generated": x[7],
                    "generation_expression": x[8],
                    "is_stored": x[9],
                    "is_hidden": x[10],
                    "is_updatable": x[11],
                    "is_system_defined": x[12],
                    "is_partitioning_column": x[13],
                    "clustering_ordinal_position": x[14],
                    "collation_name": x[15],
                    "column_default": x[16],
                    "rounding_mode": x[17],
                    "column_description": x[18],
                    "table_description": x[19],
                }
            )(row)
            for row in res["data"]
        ]

        unique_tables = {}
        for row in res:
            # generate unique table name
            table_name = row["table_name"]
            # init table if not exists
            if table_name not in unique_tables:
                table = {
                    "name": table_name,
                    "description": row["table_description"],
                    "columns": [],
                    "properties": {
                        "schema": row["table_schema"],
                        "catalog": row["table_catalog"],
                        "table": row["table_name"],
                    },
                    "primaryKey": "",
                }
                unique_tables[table_name] = CompactTable(**table)
            # table exists, and add column to the table
            column = {
                "name": row["column_name"],
                "type": row["data_type"],
                "notNull": row["is_nullable"].lower() == "no",
                "description": row["column_description"],
                "properties": {},
            }
            column = CompactColumn(**column)
            unique_tables[table_name].columns.append(column)

        compact_tables: list[CompactTable] = list(unique_tables.values())
        print(res)
        return compact_tables

    @staticmethod
    def get_postgres_table_constraints(connection_info):
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
        res = to_json(
            DataSource.postgres.get_connection(connection_info)
            .sql(sql, dialect="trino")
            .to_pandas()
        )

        # transform the result to a list of dictionaries
        res = [
            (
                lambda x: {
                    "table_schema": x[0],
                    "table_name": x[1],
                    "column_name": x[2],
                    "foreign_table_schema": x[3],
                    "foreign_table_name": x[4],
                    "foreign_column_name": x[5],
                }
            )(row)
            for row in res["data"]
        ]
        constraints = []
        for row in res:
            constraint_table = formatPostgresCompactTableName(
                row["table_schema"], row["table_name"]
            )
            constrainted_table = formatPostgresCompactTableName(
                row["foreign_table_schema"], row["foreign_table_name"]
            )
            table_name = row["table_name"]
            foreign_table_name = row["foreign_table_name"]
            column_name = row["column_name"]
            foreign_column_name = row["foreign_column_name"]
            row = {
                "constraintName": f"{table_name}_{column_name}_{foreign_table_name}_{foreign_column_name}",
                "constraintTable": constraint_table,
                "constraintColumn": column_name,
                "constraintedTable": constrainted_table,
                "constraintedColumn": foreign_column_name,
                "constraintType": ConstraintType.FOREIGN_KEY,
            }
            constraints.append(Constraint(**row))
        return constraints

    @staticmethod
    def get_bigquery_table_constraints(connection_info):
        dataset_id = connection_info.dataset_id
        sql = f"""
            SELECT 
                CONCAT(ccu.table_name, '_', ccu.column_name, '_', kcu.table_name, '_', kcu.column_name) as constraint_name,
                ccu.table_name as constraint_table, ccu.column_name constraint_column, 
                kcu.table_name as constrainted_table, kcu.column_name as constrainted_column, 
            FROM {dataset_id}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
            JOIN {dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON ccu.constraint_name = kcu.constraint_name
            JOIN {dataset_id}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            """
        res = to_json(
            DataSource.bigquery.get_connection(connection_info)
            .sql(sql, dialect="bigquery")
            .to_pandas()
        )

        # transform the result to a list of dictionaries
        response = [
            (
                lambda x: {
                    "constraintName": x[0],
                    "constraintTable": x[1],
                    "constraintColumn": x[2],
                    "constraintedTable": x[3],
                    "constraintedColumn": x[4],
                    "constraintType": ConstraintType.FOREIGN_KEY,
                }
            )(row)
            for row in res["data"]
        ]
        constraints = [Constraint(**row) for row in response]
        return constraints


def to_json(df):
    json_obj = loads(df.to_json(orient='split'))
    del json_obj['index']
    json_obj['dtypes'] = df.dtypes.apply(lambda x: x.name).to_dict()
    return json_obj


def formatPostgresCompactTableName(schema, table):
    return f"{schema}.{table}"


def transform_postgres_column_type(data_type):
    # lower case the data_type
    data_type = data_type.lower()
    print(f"=== data_type: {data_type}")

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
