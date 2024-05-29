from typing import Union

from pydantic import BaseModel, Field

from app.model.data_source import (
    PostgresConnectionInfo,
    BigQueryConnectionInfo,
    SnowflakeConnectionInfo
)


class IbisDTO(BaseModel):
    sql: str
    manifest_str: str = Field(alias="manifestStr", description="Base64 manifest")


class PostgresDTO(IbisDTO):
    connection_info: PostgresConnectionInfo = Field(alias="connectionInfo")


class BigQueryDTO(IbisDTO):
    connection_info: BigQueryConnectionInfo = Field(alias="connectionInfo")


class SnowflakeDTO(IbisDTO):
    connection_info: SnowflakeConnectionInfo = Field(alias="connectionInfo")


IbisDTO = Union[PostgresDTO, BigQueryDTO, SnowflakeDTO]
