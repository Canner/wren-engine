from enum import StrEnum, auto
from typing import List, Optional, Dict

from pydantic import BaseModel, Field


class Manifest(BaseModel):
    catalog: str
    m_schema: str = Field(alias="schema")
    models: List['Model'] = Field(default_factory=list)
    relationships: List['Relationship'] = Field(default_factory=list)
    metrics: List['Metric'] = Field(default_factory=list)
    views: List['View'] = Field(default_factory=list)


class Model(BaseModel):
    name: str
    ref_sql: str = Field(alias="refSql")
    columns: List['Column']
    base_object: Optional[str] = Field(alias="baseObject", default=None)
    table_reference: Optional[str] = Field(alias="tableReference", default=None)
    primary_key: Optional[str] = Field(alias="primaryKey", default=None)
    cached: Optional[bool] = None
    refresh_time: Optional[str] = Field(alias="refreshTime", default=None)
    properties: Optional[Dict[str, str]] = Field(default_factory=dict)


class Relationship(BaseModel):
    name: str
    models: List[str]
    join_type: 'JoinType' = Field(alias="joinType")
    condition: str
    properties: Optional[Dict[str, str]] = Field(default_factory=dict)


class Metric(BaseModel):
    name: str
    base_object: str = Field(alias="baseObject")
    dimension: List['Column']
    measure: List['Column']
    time_grain: List['TimeGrain'] = Field(alias="timeGrains", default_factory=list)
    cached: Optional[bool] = None
    refresh_time: Optional[str] = Field(alias="refreshTime", default=None)
    properties: Optional[Dict[str, str]] = Field(default_factory=dict)


class View(BaseModel):
    name: str
    statement: str
    properties: Dict[str, str] = Field(default_factory=dict)


class TimeGrain(BaseModel):
    name: str
    ref_column: str = Field(alias="refColumn")
    date_parts: List['TimeUnit'] = Field(alias="timeUnits")


class Column(BaseModel):
    name: str
    type: str
    relationship: Optional[str] = None
    is_calculated: Optional[bool] = Field(alias="isCalculated", default=False)
    not_null: Optional[bool] = Field(alias="notNull", default=False)
    expression: Optional[str] = None
    properties: Optional[Dict[str, str]] = Field(default_factory=dict)


class JoinType(StrEnum):
    ONE_TO_ONE = auto()
    ONE_TO_MANY = auto()
    MANY_TO_ONE = auto()
    MANY_TO_MANY = auto()


class TimeUnit(StrEnum):
    YEAR = auto()
    MONTH = auto()
    DAY = auto()
    HOUR = auto()
    MINUTE = auto()
    SECOND = auto()
