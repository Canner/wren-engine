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
                ON c.table_name = table_options.table_name AND table_options.OPTION_NAME = 'description'
            WHERE cf.data_type != 'GEOGRAPHY'
                AND cf.data_type NOT LIKE 'RANGE%'
            ORDER BY cf.field_path ASC
            """
        response = self.connection.sql(sql).to_pandas().to_dict(orient="records")

        def get_column(row) -> Column:
            return Column(
                # field_path supports both column & nested column
                name=row["field_path"],
                type=row["data_type"],
                notNull=row["is_nullable"].lower() == "no",
                description=row["column_description"],
                properties={},
                nestedColumns=[] if has_nested_columns(row) else None,
            )

        def get_table(row) -> Table:
            return Table(
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

        def is_root_column(row) -> bool:
            return "." not in row["field_path"]

        def has_nested_columns(row) -> bool:
            return "STRUCT" in row["data_type"]

        # eg:
        # if I would like to find the parent_column of "messages.data.elements.aspectRatio"
        # the output should be the column -> {name: "messages.data.elements", ...}
        def find_parent_column(column_metadata, root_column) -> Column:
            parent_column_names = column_metadata["field_path"].split(".")[1:-1]
            if len(parent_column_names) == 0:
                return root_column
            col_ref = root_column
            cur_column_name = root_column.name
            for partial_column_name in parent_column_names:
                cur_column_name = cur_column_name + "." + partial_column_name
                col_ref = next(
                    filter(
                        lambda column: column.name == cur_column_name,
                        col_ref.nestedColumns,
                    ),
                    None,
                )
                if not col_ref:
                    return None
            return col_ref

        unique_tables = {}

        for column_metadata in response:
            # generate unique table name
            table_name = column_metadata["table_name"]
            # init table if not exists
            if table_name not in unique_tables:
                unique_tables[table_name] = get_table(column_metadata)

            current_table = unique_tables[table_name]
            # if column is normal column, add to table
            if is_root_column(column_metadata):
                current_table.columns.append(get_column(column_metadata))
            # if column is nested column, find the parent nested column, and append to the nestedColumns of the parent column
            else:
                root_column_name = column_metadata["field_path"].split(".")[0]
                root_column = next(
                    filter(
                        lambda column: column.name == root_column_name,
                        current_table.columns,
                    ),
                    None,
                )
                if not root_column:
                    continue
                parent_nested_column = find_parent_column(column_metadata, root_column)
                if parent_nested_column:
                    parent_nested_column.nestedColumns.append(
                        get_column(column_metadata)
                    )

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
