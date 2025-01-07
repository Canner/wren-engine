import os

import duckdb
import opendal
from loguru import logger

from app.model import LocalFileConnectionInfo
from app.model.metadata.dto import (
    Column,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata


class ObjectStorageMetadata(Metadata):
    def __init__(self, connection_info):
        super().__init__(connection_info)

    def get_table_list(self) -> list[Table]:
        op = opendal.Operator("fs", root=self.connection_info.url.get_secret_value())
        conn = self._get_connection()
        unique_tables = {}
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


class LocalFileMetadata(ObjectStorageMetadata):
    def __init__(self, connection_info: LocalFileConnectionInfo):
        super().__init__(connection_info)

    def get_version(self):
        return "Local File System"
