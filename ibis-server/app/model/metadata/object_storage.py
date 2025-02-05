import os

import duckdb
import opendal
from loguru import logger

from app.model import (
    GcsFileConnectionInfo,
    LocalFileConnectionInfo,
    MinioFileConnectionInfo,
    S3FileConnectionInfo,
    UnprocessableEntityError,
)
from app.model.metadata.dto import (
    Column,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata
from app.model.utils import init_duckdb_gcs, init_duckdb_minio, init_duckdb_s3


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
            raise UnprocessableEntityError(f"Failed to list files: {e!s}")

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
        if col_type.startswith("DECIMAL"):
            return RustWrenEngineColumnType.DECIMAL

        # TODO: support struct
        if col_type.startswith("STRUCT"):
            return RustWrenEngineColumnType.UNKNOWN

        # TODO: support array
        if col_type.endswith("[]"):
            return RustWrenEngineColumnType.UNKNOWN

        # refer to https://duckdb.org/docs/sql/data_types/overview#general-purpose-data-types
        switcher = {
            "BIGINT": RustWrenEngineColumnType.INT64,
            "BIT": RustWrenEngineColumnType.INT2,
            "BLOB": RustWrenEngineColumnType.BYTES,
            "BOOLEAN": RustWrenEngineColumnType.BOOL,
            "DATE": RustWrenEngineColumnType.DATE,
            "DOUBLE": RustWrenEngineColumnType.DOUBLE,
            "FLOAT": RustWrenEngineColumnType.FLOAT,
            "INTEGER": RustWrenEngineColumnType.INT,
            # TODO: Wren engine does not support HUGEINT. Map to INT64 for now.
            "HUGEINT": RustWrenEngineColumnType.INT64,
            "INTERVAL": RustWrenEngineColumnType.INTERVAL,
            "JSON": RustWrenEngineColumnType.JSON,
            "SMALLINT": RustWrenEngineColumnType.INT2,
            "TIME": RustWrenEngineColumnType.TIME,
            "TIMESTAMP": RustWrenEngineColumnType.TIMESTAMP,
            "TIMESTAMP WITH TIME ZONE": RustWrenEngineColumnType.TIMESTAMPTZ,
            "TINYINT": RustWrenEngineColumnType.INT2,
            "UBIGINT": RustWrenEngineColumnType.INT64,
            # TODO: Wren engine does not support UHUGEINT. Map to INT64 for now.
            "UHUGEINT": RustWrenEngineColumnType.INT64,
            "UINTEGER": RustWrenEngineColumnType.INT,
            "USMALLINT": RustWrenEngineColumnType.INT2,
            "UTINYINT": RustWrenEngineColumnType.INT2,
            "UUID": RustWrenEngineColumnType.UUID,
            "VARCHAR": RustWrenEngineColumnType.STRING,
        }
        return switcher.get(col_type, RustWrenEngineColumnType.UNKNOWN)

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
        logger.debug("Initialized duckdb minio")
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
