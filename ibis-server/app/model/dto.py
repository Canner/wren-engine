from typing import Union

from pydantic import BaseModel, Field

from app.model.data_source import (
    PostgresConnectionUrl,
    PostgresConnectionInfo,
    BigQueryConnectionInfo,
    SnowflakeConnectionInfo,
)


class IbisDTO(BaseModel):
    sql: str
    manifest_str: str = Field(alias="manifestStr", description="Base64 manifest")
    column_dtypes: dict[str, str] | None = Field(
        alias="columnDtypes",
        description="If this field is set, it will forcibly convert the type.",
        default=None,
    )


class PostgresDTO(IbisDTO):
    connection_info: PostgresConnectionUrl | PostgresConnectionInfo = Field(
        alias="connectionInfo"
    )


class BigQueryDTO(IbisDTO):
    connection_info: BigQueryConnectionInfo = Field(alias="connectionInfo")


class SnowflakeDTO(IbisDTO):
    connection_info: SnowflakeConnectionInfo = Field(alias="connectionInfo")


IbisDTO = Union[PostgresDTO, BigQueryDTO, SnowflakeDTO]
