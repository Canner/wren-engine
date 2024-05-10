from pydantic import BaseModel, Field


class IbisDTO(BaseModel):
    sql: str
    pass


class PostgresDTO(IbisDTO):
    host: str = Field(examples=["localhost"])
    port: int = Field(default=5432)
    user: str
    password: str


class BigQueryDTO(IbisDTO):
    project_id: str
    dataset_id: str
    credentials: str = Field(description="Base64 encode `credentials.json`")


class SnowflakeDTO(IbisDTO):
    user: str
    password: str
    account: str
    database: str
    sf_schema: str = Field(alias="schema", default=None)  # Use `sf_schema` to avoid `schema` shadowing in parent BaseModel
