from pydantic import BaseModel, ConfigDict
from pydantic.fields import Field


class Column(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    type: str
    expression: str = None
    isCalculated: bool = False
    relationship: str = None
    notNull: bool = False
    isHidden: bool = False
    description: str = None


class TableReference(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str = None
    mdl_schema: str = Field(alias="schema", default=None)
    table: str


class Model(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    tableReference: TableReference = None
    refSql: str = None
    baseObject: str = None
    columns: list[Column] = []
    primaryKey: str = None
    cached: bool = False
    refreshTime: str = None
    properties: dict = None
    description: str = None


class Relationship(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    models: list[str]
    joinType: str = Field(alias="joinType", default="")
    condition: str = ""
    description: str = None


class View(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    statement: str
    description: str = None
    properties: dict = None


class Manifest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str = "wren"
    mdl_schema: str = Field(alias="schema", default="public")
    dataSource: str = ""
    models: list[Model] = []
    relationships: list[Relationship] = []
    views: list[View] = []
    metrics: list[dict] = []
    enumDefinitions: list[dict] = []
    description: str = None


class TableColumns(BaseModel):
    table_name: str
    column_names: list[str] = None
