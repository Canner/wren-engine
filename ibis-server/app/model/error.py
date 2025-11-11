from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
from starlette.status import (
    HTTP_404_NOT_FOUND,
    HTTP_422_UNPROCESSABLE_CONTENT,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_501_NOT_IMPLEMENTED,
    HTTP_502_BAD_GATEWAY,
    HTTP_504_GATEWAY_TIMEOUT,
)

DIALECT_SQL = "dialectSql"
PLANNED_SQL = "plannedSql"


class ErrorCode(int, Enum):
    GENERIC_USER_ERROR = 1
    NOT_FOUND = 2
    MDL_NOT_FOUND = 3
    INVALID_SQL = 4
    INVALID_MDL = 5
    DUCKDB_FILE_NOT_FOUND = 6
    ATTACH_DUCKDB_ERROR = 7
    VALIDATION_RULE_NOT_FOUND = 8
    VALIDATION_ERROR = 9
    VALIDATION_PARAMETER_ERROR = 10
    GET_CONNECTION_ERROR = 11
    INVALID_CONNECTION_INFO = 12
    GENERIC_INTERNAL_ERROR = 100
    LEGACY_ENGINE_ERROR = 101
    NOT_IMPLEMENTED = 102
    IBIS_PROJECT_ERROR = 103
    SQLGLOT_ERROR = 104
    GENERIC_EXTERNAL_ERROR = 200
    DATABASE_TIMEOUT = 201


class ErrorPhase(int, Enum):
    REQUEST_RECEIVED = 1
    MDL_EXTRACTION = 2
    SQL_PARSING = 3
    SQL_PLANNING = 4
    SQL_TRANSPILE = 5
    SQL_EXECUTION = 6
    SQL_DRY_RUN = 7
    RESPONSE_GENERATION = 8
    METADATA_FETCHING = 9
    VALIDATION = 10
    SQL_SUBSTITUTE = 11


class ErrorResponse(BaseModel):
    model_config = {"populate_by_name": True}
    error_code: str = Field(alias="errorCode")
    message: str
    metadata: dict[str, Any] | None = None
    phase: str | None = None
    timestamp: str
    correlation_id: str | None = Field(alias="correlationId", default=None)


class WrenError(Exception):
    error_code: ErrorCode
    message: str
    phase: ErrorPhase | None = None
    metadata: dict[str, Any] | None = None
    timestamp: str | None = None

    def __init__(
        self,
        error_code: ErrorCode,
        message: str,
        phase: ErrorPhase | None = None,
        metadata: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ):
        self.error_code = error_code
        self.message = message
        self.phase = phase
        self.metadata = metadata
        self.timestamp = datetime.now().isoformat()

    def get_response(self, correlation_id: str | None = None) -> ErrorResponse:
        return ErrorResponse(
            error_code=self.error_code.name,
            message=self.message,
            metadata=self.metadata,
            phase=self.phase.name if self.phase else None,
            timestamp=self.timestamp,
            correlation_id=correlation_id,
        )

    def get_http_status_code(self) -> int:
        match self.error_code:
            case (
                ErrorCode.NOT_FOUND
                | ErrorCode.MDL_NOT_FOUND
                | ErrorCode.VALIDATION_RULE_NOT_FOUND
            ):
                return HTTP_404_NOT_FOUND
            case ErrorCode.NOT_IMPLEMENTED:
                return HTTP_501_NOT_IMPLEMENTED
            case ErrorCode.GENERIC_EXTERNAL_ERROR:
                return HTTP_502_BAD_GATEWAY
            case ErrorCode.DATABASE_TIMEOUT:
                return HTTP_504_GATEWAY_TIMEOUT
            case e:
                if e.value < 100:
                    return HTTP_422_UNPROCESSABLE_CONTENT
                return HTTP_500_INTERNAL_SERVER_ERROR


class DatabaseTimeoutError(WrenError):
    def __init__(
        self,
        message: str,
    ):
        enhanced_message = f"{message!s}.\nIt seems your database is not responding or the query is taking too long to execute. Please check your database status and query performance."
        super().__init__(
            error_code=ErrorCode.DATABASE_TIMEOUT,
            message=enhanced_message,
        )
