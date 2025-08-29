import os

import duckdb
import opendal
import pyarrow as pa
from loguru import logger

from app.model import (
    GcsFileConnectionInfo,
    LocalFileConnectionInfo,
    MinioFileConnectionInfo,
    S3FileConnectionInfo,
)
from app.model.connector import DuckDBConnector
from app.model.error import ErrorCode, ErrorPhase, WrenError
from app.model.metadata.dto import (
    Column,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata
from app.model.utils import init_duckdb_gcs, init_duckdb_minio, init_duckdb_s3

DUCKDB_TYPE_MAPPING = {
    "bigint": RustWrenEngineColumnType.INT64,
    "bit": RustWrenEngineColumnType.INT2,
    "blob": RustWrenEngineColumnType.BYTES,
    "boolean": RustWrenEngineColumnType.BOOL,
    "date": RustWrenEngineColumnType.DATE,
    "double": RustWrenEngineColumnType.DOUBLE,
    "float": RustWrenEngineColumnType.FLOAT,
    "integer": RustWrenEngineColumnType.INT,
    # TODO: Wren engine does not support HUGEINT. Map to INT64 for now.
    "hugeint": RustWrenEngineColumnType.INT64,
    "interval": RustWrenEngineColumnType.INTERVAL,
    "json": RustWrenEngineColumnType.JSON,
    "smallint": RustWrenEngineColumnType.INT2,
    "time": RustWrenEngineColumnType.TIME,
    "timestamp": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp with time zone": RustWrenEngineColumnType.TIMESTAMPTZ,
    "tinyint": RustWrenEngineColumnType.INT2,
    "ubigint": RustWrenEngineColumnType.INT64,
    # TODO: Wren engine does not support UHUGEINT. Map to INT64 for now.
    "uhugeint": RustWrenEngineColumnType.INT64,
    "uinteger": RustWrenEngineColumnType.INT,
    "usmallint": RustWrenEngineColumnType.INT2,
    "utinyint": RustWrenEngineColumnType.INT2,
    "uuid": RustWrenEngineColumnType.UUID,
    "varchar": RustWrenEngineColumnType.STRING,
}


class ObjectStorageMetadata(Metadata):
    def __init__(self, connection_info):
        super().__init__(connection_info)

    def get_table_list(self) -> list[Table]:
        op = self._get_dal_operator()
        conn = self._get_connection()
        unique_tables = {}
        try:
            for file in op.list("/"):
                if file.path != "/":
                    stat = op.stat(file.path)
                    if stat.mode.is_dir():
                        # if the file is a directory, use the directory name as the table name
                        table_name = os.path.basename(os.path.normpath(file.path))
                        full_path = f"{self.connection_info.url.get_secret_value()}/{table_name}/*.{self.connection_info.format}"
                    else:
                        # if the file is a file, use the file name as the table name
                        table_name = os.path.splitext(os.path.basename(file.path))[0]
                        full_path = (
                            f"{self.connection_info.url.get_secret_value()}/{file.path}"
                        )

                    # add required prefix for object storage
                    full_path = self._get_full_path(full_path)
                    # read the file with the target format if unreadable, skip the file
                    df = self._read_df(conn, full_path)
                    if df is None:
                        continue
                    columns = []
                    try:
                        for col in df.columns:
                            duckdb_type = df[col].dtypes[0]
                            columns.append(
                                Column(
                                    name=col,
                                    type=self._to_column_type(duckdb_type.__str__()),
                                    notNull=False,
                                )
                            )
                    except Exception as e:
                        logger.debug(f"Failed to read column types: {e}")
                        continue

                    unique_tables[table_name] = Table(
                        name=table_name,
                        description=None,
                        columns=[],
                        properties=TableProperties(
                            table=table_name,
                            schema=None,
                            catalog=None,
                            path=full_path,
                        ),
                        primaryKey=None,
                    )
                    unique_tables[table_name].columns = columns
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                f"Failed to list files: {e!s}",
                phase=ErrorPhase.METADATA_FETCHING,
            )

        return list(unique_tables.values())

    def get_constraints(self):
        return []

    def get_version(self):
        raise NotImplementedError("Subclasses must implement `get_version` method")

    def _read_df(self, conn, path):
        if self.connection_info.format == "parquet":
            try:
                return conn.read_parquet(path)
            except Exception as e:
                logger.debug(f"Failed to read parquet file: {e}")
                return None
        elif self.connection_info.format == "csv":
            try:
                logger.debug(f"Reading csv file: {path}")
                return conn.read_csv(path)
            except Exception as e:
                logger.debug(f"Failed to read csv file: {e}")
                return None
        elif self.connection_info.format == "json":
            try:
                return conn.read_json(path)
            except Exception as e:
                logger.debug(f"Failed to read json file: {e}")
                return None
        else:
            raise NotImplementedError(
                f"Unsupported format: {self.connection_info.format}"
            )

    def _to_column_type(self, col_type: str) -> RustWrenEngineColumnType:
        """Transform DuckDB data type to RustWrenEngineColumnType.

        Args:
            col_type: The DuckDB data type string

        Returns:
            The corresponding RustWrenEngineColumnType
        """
        if col_type.startswith("DECIMAL"):
            return RustWrenEngineColumnType.DECIMAL

        # TODO: support struct
        if col_type.startswith("STRUCT"):
            return RustWrenEngineColumnType.UNKNOWN

        # TODO: support array
        if col_type.endswith("[]"):
            return RustWrenEngineColumnType.UNKNOWN
        # Convert to lowercase for comparison
        normalized_type = col_type.lower()

        # Use the module-level mapping table
        mapped_type = DUCKDB_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown DuckDB data type: {col_type}")

        return mapped_type

    def _get_connection(self):
        return duckdb.connect()

    def _get_dal_operator(self):
        return opendal.Operator("fs", root=self.connection_info.url.get_secret_value())

    def _get_full_path(self, path):
        return path


class LocalFileMetadata(ObjectStorageMetadata):
    def __init__(self, connection_info: LocalFileConnectionInfo):
        super().__init__(connection_info)

    def get_version(self):
        return "Local File System"


class S3FileMetadata(ObjectStorageMetadata):
    def __init__(self, connection_info: S3FileConnectionInfo):
        super().__init__(connection_info)

    def get_version(self):
        return "S3"

    def _get_connection(self):
        conn = duckdb.connect()
        init_duckdb_s3(conn, self.connection_info)
        logger.debug("Initialized duckdb s3")
        return conn

    def _get_dal_operator(self):
        info: S3FileConnectionInfo = self.connection_info
        return opendal.Operator(
            "s3",
            root=info.url.get_secret_value(),
            bucket=info.bucket.get_secret_value(),
            region=info.region.get_secret_value(),
            secret_access_key=info.secret_key.get_secret_value(),
            access_key_id=info.access_key.get_secret_value(),
        )

    def _get_full_path(self, path):
        if path.startswith("/"):
            path = path[1:]

        return f"s3://{self.connection_info.bucket.get_secret_value()}/{path}"


class MinioFileMetadata(ObjectStorageMetadata):
    def __init__(self, connection_info: MinioFileConnectionInfo):
        super().__init__(connection_info)

    def get_version(self):
        return "Minio"

    def _get_connection(self):
        conn = duckdb.connect()
        init_duckdb_minio(conn, self.connection_info)
        logger.debug("Initialized duckdb minio")
        return conn

    def _get_dal_operator(self):
        info: MinioFileConnectionInfo = self.connection_info

        if info.ssl_enabled:
            endpoint = f"https://{info.endpoint.get_secret_value()}"
        else:
            endpoint = f"http://{info.endpoint.get_secret_value()}"

        return opendal.Operator(
            "s3",
            root=info.url.get_secret_value(),
            bucket=info.bucket.get_secret_value(),
            region="ap-northeast-1",
            endpoint=endpoint,
            secret_access_key=info.secret_key.get_secret_value(),
            access_key_id=info.access_key.get_secret_value(),
        )

    def _get_full_path(self, path):
        if path.startswith("/"):
            path = path[1:]

        return f"s3://{self.connection_info.bucket.get_secret_value()}/{path}"


class GcsFileMetadata(ObjectStorageMetadata):
    def __init__(self, connection_info: GcsFileConnectionInfo):
        super().__init__(connection_info)

    def get_version(self):
        return "GCS"

    def _get_connection(self):
        conn = duckdb.connect()
        init_duckdb_gcs(conn, self.connection_info)
        logger.debug("Initialized duckdb gcs")
        return conn

    def _get_dal_operator(self):
        info: GcsFileConnectionInfo = self.connection_info

        return opendal.Operator(
            "gcs",
            root=info.url.get_secret_value(),
            bucket=info.bucket.get_secret_value(),
            credential=info.credentials.get_secret_value(),
        )

    def _get_full_path(self, path):
        if path.startswith("/"):
            path = path[1:]

        return f"gs://{self.connection_info.bucket.get_secret_value()}/{path}"


class DuckDBMetadata(ObjectStorageMetadata):
    def __init__(self, connection_info: LocalFileConnectionInfo):
        super().__init__(connection_info)
        self.connection = DuckDBConnector(connection_info)

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
                information_schema.columns c
                ON t.table_schema = c.table_schema
                AND t.table_name = c.table_name
            WHERE
                t.table_type IN ('BASE TABLE', 'VIEW')
                AND t.table_schema NOT IN ('information_schema', 'pg_catalog');
            """
        response = (
            self.connection.query(sql, limit=None).to_pandas().to_dict(orient="records")
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
                    type=self._to_column_type(row["data_type"]),
                    notNull=row["is_nullable"].lower() == "no",
                    properties=None,
                )
            )
        return list(unique_tables.values())

    def _format_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def get_constraints(self):
        return []

    def get_version(self):
        df: pa.Table = self.connection.query("SELECT version()")
        if df is None:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR, "Failed to get DuckDB version"
            )
        if df.num_rows == 0:
            raise WrenError(ErrorCode.GENERIC_USER_ERROR, "DuckDB version is empty")
        return df.column(0).to_pylist()[0]
