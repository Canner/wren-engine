from pydantic import BaseModel


class TranspileDTO(BaseModel):
    sql: str
    read: str
    write: str
