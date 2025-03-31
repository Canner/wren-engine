from pydantic import BaseModel
from pydantic.fields import Field


class Column(BaseModel):
    name: str
    type: str
    expression: str = None
    isCalculated: bool = False
    relationship: str = None
    description: str = None


class TableReference(BaseModel):
    catalog: str = None
    mdl_schema: str = Field(alias="schema", default=None)
    table: str


class Model(BaseModel):
    name: str
    tableReference: TableReference
    columns: list[Column]
    primaryKey: str = None
    description: str = None


class Relationship(BaseModel):
    name: str
    models: list[str]
    join_type: str
    join_condition: str


class View(BaseModel):
    name: str
    statement: str
    description: str = None


class Manifest(BaseModel):
    catalog: str = "wren"
    mdl_schema: str = Field(alias="schema", default="public")
    models: list[Model]
    relationships: list[Relationship]
    views: list[View]
    description: str = None


class TableColumns(BaseModel):
    table_name: str
    column_names: list[str] = None
