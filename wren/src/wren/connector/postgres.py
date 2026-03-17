from contextlib import suppress

import pyarrow as pa
from loguru import logger

from wren.connector.base import IbisConnector
from wren.model.data_source import DataSource
from wren.model.error import DIALECT_SQL, ErrorCode, ErrorPhase, WrenError


class PostgresConnector(IbisConnector):
    def __init__(self, connection_info):
        super().__init__(DataSource.postgres, connection_info)

    def query(self, sql: str, limit: int | None = None) -> pa.Table:
        import psycopg  # noqa: PLC0415

        try:
            return super().query(sql, limit)
        except psycopg.errors.QueryCanceled:
            raise
        except (WrenError, TimeoutError):
            raise
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                str(e),
                phase=ErrorPhase.SQL_EXECUTION,
                metadata={DIALECT_SQL: sql},
            ) from e

    def dry_run(self, sql: str) -> None:
        import psycopg  # noqa: PLC0415

        try:
            super().dry_run(sql)
        except psycopg.errors.QueryCanceled:
            raise
        except (WrenError, TimeoutError):
            raise
        except Exception as e:
            raise WrenError(
                ErrorCode.GENERIC_USER_ERROR,
                str(e),
                phase=ErrorPhase.SQL_DRY_RUN,
                metadata={DIALECT_SQL: sql},
            ) from e

    def close(self) -> None:
        if self._closed or not hasattr(self, "connection") or self.connection is None:
            return
        try:
            if hasattr(self.connection, "con") and self.connection.con is not None:
                if (
                    hasattr(self.connection.con, "closed")
                    and not self.connection.con.closed
                ):
                    with suppress(Exception):
                        self.connection.con.cancel()
                    import time  # noqa: PLC0415

                    time.sleep(0.1)
                    self.connection.con.close()
            elif hasattr(self.connection, "close"):
                self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing postgres connection: {e}")
            if hasattr(self.connection, "con"):
                self.connection.con = None
        finally:
            self._closed = True
            self.connection = None


def create_connector(connection_info) -> PostgresConnector:
    return PostgresConnector(connection_info)
