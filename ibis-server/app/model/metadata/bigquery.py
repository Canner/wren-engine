from google.api_core.exceptions import Forbidden, NotFound
from loguru import logger

from app.model import (
    BigQueryDatasetConnectionInfo,
    BigQueryProjectConnectionInfo,
)
from app.model.connector import BigQueryConnector
from app.model.error import ErrorCode, WrenError
from app.model.metadata.dto import (
    BigQueryFilterInfo,
    Catalog,
    Column,
    Constraint,
    ConstraintType,
    FilterInfo,
    ProjectDatasets,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

# BigQuery-specific type mapping
BIGQUERY_TYPE_MAPPING = {
    # GEOGRAPHY and RANGE columns were filtered out
    "bool": RustWrenEngineColumnType.BOOL,
    "boolean": RustWrenEngineColumnType.BOOL,
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

BIGQUERY_PUBLIC_DATASET_PROJECT_ID = "bigquery-public-data"


class BigQueryMetadata(Metadata):
    def __init__(self, connection_info: BigQueryDatasetConnectionInfo):
        super().__init__(connection_info)
        self.connection = BigQueryConnector(connection_info)

    def get_table_list(
        self,
        filter_info: FilterInfo | None = None,
        limit: int | None = None,
    ) -> list[Table]:
        project_datasets = self._get_project_datasets(filter_info)
        self._validate_project_datasets_for_table_list(project_datasets)

        def build_list_table_sql(qualifier: str) -> str:
            sql = f"""
                SELECT 
                    c.table_catalog,
                    c.table_schema,
                    c.table_name,
                    table_options.option_value AS table_description,
                    ARRAY_AGG(STRUCT(
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
                        cf.description AS column_description
                    )  ORDER BY cf.field_path ASC
                ) AS columns
                FROM {qualifier}.INFORMATION_SCHEMA.COLUMNS c 
                JOIN {qualifier}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS cf 
                    ON cf.table_name = c.table_name 
                    AND cf.column_name = c.column_name
                LEFT JOIN {qualifier}.INFORMATION_SCHEMA.TABLE_OPTIONS table_options
                    ON c.table_name = table_options.table_name AND table_options.OPTION_NAME = 'description'
                WHERE cf.data_type != 'GEOGRAPHY'
                    AND cf.data_type NOT LIKE 'RANGE%'
                """
            if (
                filter_info
                and hasattr(filter_info, "filter_pattern")
                and filter_info.filter_pattern
            ):
                sql += f"\nAND REGEXP_CONTAINS(c.table_name, r'{filter_info.filter_pattern}') "

            if (
                isinstance(self.connection_info, BigQueryProjectConnectionInfo)
                and project_dataset.dataset_ids
                and len(project_dataset.dataset_ids) > 0
            ):
                dataset_list = ", ".join(
                    [f"'{ds_id}'" for ds_id in project_dataset.dataset_ids]
                )
                sql += f"\nAND c.table_schema in ({dataset_list})"
            sql += "\nGROUP BY c.table_catalog, c.table_schema, c.table_name, table_description"
            return sql

        project_sqls = []
        for project_dataset in project_datasets:
            if project_dataset.project_id == BIGQUERY_PUBLIC_DATASET_PROJECT_ID:
                for dataset in project_dataset.dataset_ids:
                    self._validate_dataset_region(project_dataset.project_id, dataset)
                    qualifier = f"`{project_dataset.project_id}.{dataset}`"
                    sql = build_list_table_sql(qualifier)
                    project_sqls.append(sql)
            else:
                qualifier = f"`{project_dataset.project_id}.{self._get_schema_qualifier(project_dataset.dataset_ids)}`"
                sql = build_list_table_sql(qualifier)
                project_sqls.append(sql)

        union_sql = "\nUNION ALL\n".join(project_sqls)

        if limit is not None:
            union_sql += f"\n LIMIT {limit}"

        logger.debug(f"get table list SQL: {union_sql}")

        response = self.connection.query(union_sql).to_pylist()

        table_list = []
        is_multiple_projects = len(project_datasets) > 1
        # if multiple datasets exist, we need to include schema in the table name (include catalog if multiple projects exist)
        is_multiple_datasets = is_multiple_projects or (
            isinstance(self.connection_info, BigQueryProjectConnectionInfo)
            and len(project_datasets[0].dataset_ids or []) > 1
        )

        for table_metadata in response:
            # generate unique table name
            table_name = self._format_compact_table_name(
                table_metadata["table_catalog"] if is_multiple_projects else None,
                table_metadata["table_schema"] if is_multiple_datasets else None,
                table_metadata["table_name"],
            )
            table = self.get_table(table_metadata, table_name)

            # if column is normal column, add to table

            for column_metadata in table_metadata["columns"]:
                if self.is_root_column(column_metadata):
                    table.columns.append(self.get_column(column_metadata))
                # if column is nested column, find the parent nested column, and append to the nestedColumns of the parent column
                else:
                    root_column_name = column_metadata["field_path"].split(".")[0]
                    root_column = next(
                        filter(
                            lambda column: column.name == root_column_name,
                            table.columns,
                        ),
                        None,
                    )
                    if not root_column:
                        continue
                    parent_nested_column = self.find_parent_column(
                        column_metadata, root_column
                    )
                    if parent_nested_column:
                        if not parent_nested_column.nestedColumns:
                            parent_nested_column.nestedColumns = []
                        parent_nested_column.nestedColumns.append(
                            self.get_column(column_metadata)
                        )
            table_list.append(table)

        if len(table_list) == 0:
            raise WrenError(
                ErrorCode.NOT_FOUND,
                "No tables found in the specified dataset. Please check your permissions and name of dataset and project.",
            )

        return table_list

    def _validate_dataset_region(self, project_id: str, dataset_id: str) -> bool:
        dataset = self.connection.connection.get_dataset(f"{project_id}.{dataset_id}")
        dataset_location = dataset.location.lower()
        connection_region = self.connection_info.region.get_secret_value().lower()
        if dataset_location != connection_region:
            raise WrenError(
                ErrorCode.INVALID_CONNECTION_INFO,
                f"Dataset {project_id}.{dataset_id} is in region {dataset_location}, which does not match the connection region {connection_region}.",
            )

    def get_schema_list(self, filter_info=None, limit=None):
        if isinstance(self.connection_info, BigQueryDatasetConnectionInfo):
            return [
                Catalog(
                    name=self.connection_info.get_billing_project_id(),
                    schemas=[self.connection_info.dataset_id.get_secret_value()],
                )
            ]
        project_set = set()
        project_set.add(self.connection_info.get_billing_project_id())
        project_datasets = self._get_project_datasets(filter_info)
        used_bigquery_public_data = False
        if project_datasets is not None:
            for pd in project_datasets:
                if pd.project_id == BIGQUERY_PUBLIC_DATASET_PROJECT_ID:
                    used_bigquery_public_data = True
                    continue
                project_set.add(pd.project_id)

        project_sqls = []
        for project in list(project_set):
            qualifier = (
                f"`{project}.region-{self.connection_info.region.get_secret_value()}`"
            )
            # filter out columns with GEOGRAPHY & RANGE types
            sql = f"""
                SELECT
                    catalog_name,
                    schema_name
                FROM {qualifier}.INFORMATION_SCHEMA.SCHEMATA
                """
            project_sqls.append(sql)

        union_sql = "\nUNION ALL\n".join(project_sqls)

        if limit is not None:
            union_sql += f"\n LIMIT {limit}"

        grouping_sql = f"""
            SELECT catalog_name, array_agg(schema_name) AS schema_names
            FROM (
                {union_sql}
            )
            GROUP BY catalog_name
        """
        logger.debug(f"get schema list SQL: {grouping_sql}")

        response = self.connection.query(grouping_sql).to_pylist()

        if len(response) == 0:
            raise WrenError(
                ErrorCode.NOT_FOUND,
                "No tables found in the specified dataset. Please check your permissions and name of dataset and project.",
            )
        project_list = []
        for row in response:
            project_list.append(
                Catalog(name=row["catalog_name"], schemas=row["schema_names"])
            )

        if used_bigquery_public_data:
            public_data_set = self.connection.connection.list_datasets(
                project=BIGQUERY_PUBLIC_DATASET_PROJECT_ID
            )
            dataset_list = [dataset.dataset_id for dataset in public_data_set]
            project_list.append(
                Catalog(
                    name=BIGQUERY_PUBLIC_DATASET_PROJECT_ID,
                    schemas=dataset_list,
                )
            )

        return project_list

    def get_column(self, row) -> Column:
        return Column(
            # field_path supports both column & nested column
            name=row["field_path"],
            type=row["data_type"],
            notNull=row["is_nullable"].lower() == "no",
            description=row["column_description"],
            properties={
                "column_order": row["ordinal_position"],
            },
            nestedColumns=[] if self.has_nested_columns(row) else None,
        )

    def get_table(self, row, table_name: str) -> Table:
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

    def is_root_column(self, row) -> bool:
        return "." not in row["field_path"]

    def has_nested_columns(self, row) -> bool:
        return "STRUCT" in row["data_type"]

    def _format_compact_table_name(
        self,
        catalog: str | None,
        schema: str | None,
        table: str,
        delimiter: str = ".",
    ) -> str:
        if schema is None or schema == "":
            return f"{table}"
        if catalog is None or catalog == "":
            return f"{schema}{delimiter}{table}"
        return f"{catalog}{delimiter}{schema}{delimiter}{table}"

    # eg:
    # if I would like to find the parent_column of "messages.data.elements.aspectRatio"
    # the output should be the column -> {name: "messages.data.elements", ...}
    def find_parent_column(self, column_metadata, root_column) -> Column:
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

    def get_constraints(
        self, filter_info: FilterInfo | None = None
    ) -> list[Constraint]:
        project_datasets = self._get_project_datasets(filter_info)
        constraints = []

        for project_dataset in project_datasets:
            project_id = project_dataset.project_id
            schema_qualifier = self._get_schema_qualifier(project_dataset.dataset_ids)
            qualifier = f"`{project_id}.{schema_qualifier}`"
            sql = f"""
                SELECT 
                    tc.table_catalog as start_catalog,
                    tc.table_schema as start_schema,
                    tc.table_name as start_table,
                    kcu.column_name as start_column,
                    ccu.table_catalog as end_catalog,
                    ccu.table_schema as end_schema,
                    ccu.table_name as end_table,
                    ccu.column_name as end_column
                FROM {qualifier}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN {qualifier}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                    ON tc.constraint_catalog = ccu.constraint_catalog
                    AND tc.constraint_schema = ccu.constraint_schema
                    AND tc.constraint_name = ccu.constraint_name
                JOIN {qualifier}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.constraint_catalog = kcu.constraint_catalog
                    AND tc.constraint_schema = kcu.constraint_schema
                    AND tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                """

            logger.debug(f"get constraints SQL: {sql}")

            try:
                response = self.connection.query(sql).to_pylist()
            except (Forbidden, NotFound):
                logger.warning(
                    f"Can't access dataset {schema_qualifier} in project {project_id}"
                )
                continue

            is_multiple_projects = len(project_datasets) > 1
            # if multiple datasets exist, we need to include schema in the table name (include catalog if multiple projects exist)
            is_multiple_datasets = is_multiple_projects or (
                isinstance(self.connection_info, BigQueryProjectConnectionInfo)
                and len(project_dataset.dataset_ids or []) > 1
            )

            for row in response:
                start_catalog = row["start_catalog"] if is_multiple_projects else None
                start_schema = row["start_schema"] if is_multiple_datasets else None
                start_table = self._format_compact_table_name(
                    start_catalog, start_schema, row["start_table"]
                )
                end_catalog = row["end_catalog"] if is_multiple_projects else None
                end_schema = row["end_schema"] if is_multiple_datasets else None
                end_table = self._format_compact_table_name(
                    end_catalog, end_schema, row["end_table"]
                )

                start_table_underline = self._format_compact_table_name(
                    start_catalog, start_schema, row["start_table"], delimiter="_"
                )
                end_table_underline = self._format_compact_table_name(
                    end_catalog, end_schema, row["end_table"], delimiter="_"
                )
                constraints.append(
                    Constraint(
                        constraintName=f"{start_table_underline}_{row['start_column']}_{end_table_underline}_{row['end_column']}",
                        constraintTable=start_table,
                        constraintColumn=row["start_column"],
                        constraintedTable=end_table,
                        constraintedColumn=row["end_column"],
                        constraintType=ConstraintType.FOREIGN_KEY,
                    )
                )
        return constraints

    def _get_project_datasets(
        self, filter: FilterInfo | None
    ) -> list["ProjectDatasets"]:
        if isinstance(self.connection_info, BigQueryDatasetConnectionInfo):
            return [
                ProjectDatasets(
                    projectId=self.connection_info.get_billing_project_id(),
                    datasetIds=[self.connection_info.dataset_id.get_secret_value()],
                )
            ]
        elif isinstance(self.connection_info, BigQueryProjectConnectionInfo):
            if filter is not None and isinstance(filter, BigQueryFilterInfo):
                return filter.projects
            return [
                ProjectDatasets(
                    projectId=self.connection_info.get_billing_project_id(),
                    datasetIds=None,
                )
            ]
        raise WrenError(
            ErrorCode.GENERIC_INTERNAL_ERROR, "Invalid connection info type"
        )

    def _get_schema_qualifier(self, dataset_ids):
        if isinstance(self.connection_info, BigQueryDatasetConnectionInfo):
            return dataset_ids[0]
        elif isinstance(self.connection_info, BigQueryProjectConnectionInfo):
            return f"region-{self.connection_info.region.get_secret_value()}"
        raise WrenError(
            ErrorCode.GENERIC_INTERNAL_ERROR, "Invalid connection info type"
        )

    def _validate_project_datasets_for_table_list(
        self, project_datasets: list["ProjectDatasets"]
    ):
        if len(project_datasets) == 0:
            raise WrenError(
                ErrorCode.INVALID_CONNECTION_INFO,
                "At least one project and dataset must be specified",
            )
        for project_dataset in project_datasets:
            if project_dataset.project_id is None or project_dataset.project_id == "":
                raise WrenError(
                    ErrorCode.INVALID_CONNECTION_INFO, "project id should not be empty"
                )
            if (
                project_dataset.dataset_ids is None
                or len(project_dataset.dataset_ids) == 0
            ):
                raise WrenError(
                    ErrorCode.INVALID_CONNECTION_INFO, "dataset ids should not be empty"
                )
            for dataset_id in project_dataset.dataset_ids:
                if dataset_id is None or dataset_id == "":
                    raise WrenError(
                        ErrorCode.INVALID_CONNECTION_INFO,
                        "dataset id should not be empty",
                    )

    def get_version(self) -> str:
        return "Follow BigQuery release version"

    def _transform_column_type(self, data_type: str) -> str | RustWrenEngineColumnType:
        """Transform BigQuery data type to RustWrenEngineColumnType.

        Args:
            data_type: The BigQuery data type string

        Returns:
            The corresponding RustWrenEngineColumnType or original string for complex types
        """
        # Convert to lowercase for comparison
        normalized_type = data_type.lower()

        # Handle complex types (array, struct) by returning as-is
        if normalized_type.startswith(("array", "struct")):
            return data_type

        # Map to RustWrenEngineColumnType using module-level mapping
        mapped_type = BIGQUERY_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown BigQuery data type: {data_type}")

        return mapped_type
