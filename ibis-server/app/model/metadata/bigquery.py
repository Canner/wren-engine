from app.model import BigQueryConnectionInfo
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


class BigQueryMetadata(Metadata):
    def __init__(self, connection_info: BigQueryConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.bigquery.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        dataset_id = self.connection_info.dataset_id.get_secret_value()

        # filter out columns with GEOGRAPHY & RANGE types
        sql = f"""
            SELECT 
                c.table_catalog,
                c.table_schema,
                c.table_name,
                c.column_name,
                c.ordinal_position,
                c.is_nullable,
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
                cf.data_type,
                cf.field_path,
                cf.description AS column_description, 
                table_options.option_value AS table_description
            FROM {dataset_id}.INFORMATION_SCHEMA.COLUMNS c 
            JOIN {dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS cf 
                ON cf.table_name = c.table_name 
                AND cf.column_name = c.column_name
            LEFT JOIN {dataset_id}.INFORMATION_SCHEMA.TABLE_OPTIONS table_options
                ON c.table_name = table_options.table_name
            WHERE cf.data_type != 'GEOGRAPHY'
                AND cf.data_type NOT LIKE 'RANGE%'
            """
        response = self.connection.sql(sql).to_pandas().to_dict(orient="records")

        def get_column(row, nestedColumns=None) -> Column:
            return Column(
                # field_path supports both column & nested column
                name=row["field_path"],
                type=row["data_type"],
                notNull=row["is_nullable"].lower() == "no",
                description=row["column_description"],
                properties={},
                nestedColumns=nestedColumns,
            )

        unique_tables = {}
        current_struct = None
        for index, row in enumerate(response):
            # generate unique table name
            table_name = row["table_name"]
            # init table if not exists
            if table_name not in unique_tables:
                unique_tables[table_name] = Table(
                    name=table_name,
                    description=row["table_description"],
                    columns=[],
                    properties=TableProperties(
                        schema=row["table_schema"],
                        catalog=row["table_catalog"],
                        table=row["table_name"],
                    ),
                    primaryKey="",
                )
            # table exists, and add column to the table
            if "STRUCT" in row["data_type"]:
                if current_struct:
                    current_struct.nestedColumns.append(get_column(row, []))
                    current_struct = current_struct.nestedColumns[-1]
                else:
                    unique_tables[table_name].columns.append(get_column(row, []))
                    current_struct = unique_tables[table_name].columns[-1]
            elif current_struct:
                # add nested column to current_struct
                current_struct.nestedColumns.append(get_column(row))
            else:
                # add table column
                unique_tables[table_name].columns.append(get_column(row))
            # reset current_struct if next column name different
            is_not_last_item = index + 1 < len(response)
            if (
                is_not_last_item
                and response[index + 1].get("column_name") != row["column_name"]
            ):
                current_struct = None

        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        dataset_id = self.connection_info.dataset_id.get_secret_value()
        sql = f"""
            SELECT 
                CONCAT(ccu.table_name, '_', ccu.column_name, '_', kcu.table_name, '_', kcu.column_name) as constraintName,
                ccu.table_name as constraintTable, ccu.column_name constraintColumn, 
                kcu.table_name as constraintedTable, kcu.column_name as constraintedColumn, 
            FROM {dataset_id}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
            JOIN {dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON ccu.constraint_name = kcu.constraint_name
            JOIN {dataset_id}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            """
        response = self.connection.sql(sql).to_pandas().to_dict(orient="records")

        constraints = []
        for row in response:
            constraints.append(
                Constraint(
                    constraintName=row["constraintName"],
                    constraintTable=row["constraintTable"],
                    constraintColumn=row["constraintColumn"],
                    constraintedTable=row["constraintedTable"],
                    constraintedColumn=row["constraintedColumn"],
                    constraintType=ConstraintType.FOREIGN_KEY,
                )
            )
        return constraints

    def get_version(self) -> str:
        return "Follow BigQuery release version"

    def _transform_column_type(self, data_type):
        # lower case the data_type
        data_type = data_type.lower()

        # if data_type start with "array" or "struct", by pass it
        if data_type.startswith(("array", "struct")):
            return data_type

        # Map BigQuery types to RustWrenEngineColumnType
        switcher = {
            # GEOGRAPHY and RANGE columns were filtered out
            "bytes": RustWrenEngineColumnType.BYTES,
            "date": RustWrenEngineColumnType.DATE,
            "datetime": RustWrenEngineColumnType.DATETIME,
            "interval": RustWrenEngineColumnType.INTERVAL,
            "json": RustWrenEngineColumnType.JSON,
            "int64": RustWrenEngineColumnType.INT64,
            "numeric": RustWrenEngineColumnType.NUMERIC,
            "bignumeric": RustWrenEngineColumnType.BIGNUMERIC,
            "float64": RustWrenEngineColumnType.FLOAT64,
            "string": RustWrenEngineColumnType.STRING,
            "time": RustWrenEngineColumnType.TIME,
            "timestamp": RustWrenEngineColumnType.TIMESTAMPTZ,
        }

        return switcher.get(data_type, RustWrenEngineColumnType.UNKNOWN)
