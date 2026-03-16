import importlib

from wren.model.data_source import DataSource
from wren.model.error import ErrorCode, WrenError

_REGISTRY: dict[DataSource, str] = {
    DataSource.postgres: "wren.connector.postgres",
    DataSource.mysql: "wren.connector.mysql",
    DataSource.doris: "wren.connector.mysql",
    DataSource.mssql: "wren.connector.mssql",
    DataSource.canner: "wren.connector.canner",
    DataSource.bigquery: "wren.connector.bigquery",
    DataSource.local_file: "wren.connector.duckdb",
    DataSource.s3_file: "wren.connector.duckdb",
    DataSource.minio_file: "wren.connector.duckdb",
    DataSource.gcs_file: "wren.connector.duckdb",
    DataSource.duckdb: "wren.connector.duckdb",
    DataSource.redshift: "wren.connector.redshift",
    DataSource.spark: "wren.connector.spark",
    DataSource.databricks: "wren.connector.databricks",
    DataSource.trino: "wren.connector.ibis",
    DataSource.clickhouse: "wren.connector.ibis",
    DataSource.oracle: "wren.connector.ibis",
    DataSource.snowflake: "wren.connector.ibis",
    DataSource.athena: "wren.connector.ibis",
}

_NEEDS_DATA_SOURCE = {
    DataSource.mysql,
    DataSource.doris,
    DataSource.trino,
    DataSource.clickhouse,
    DataSource.oracle,
    DataSource.snowflake,
    DataSource.athena,
}


def get_connector(data_source: DataSource, connection_info):
    module_path = _REGISTRY.get(data_source)
    if module_path is None:
        raise WrenError(
            ErrorCode.NOT_IMPLEMENTED,
            f"Unsupported data source: {data_source}",
        )

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise WrenError(
            ErrorCode.NOT_IMPLEMENTED,
            f"Connector '{data_source.value}' requires additional dependencies: {e}. "
            f"Install with: pip install wren[{data_source.value}]",
        ) from e

    if data_source in _NEEDS_DATA_SOURCE:
        return module.create_connector(data_source, connection_info)
    return module.create_connector(connection_info)
