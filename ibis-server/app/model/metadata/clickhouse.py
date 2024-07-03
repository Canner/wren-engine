from json import loads

from app.model import ClickHouseConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    Table,
    TableProperties,
    WrenEngineColumnType,
)
from app.model.metadata.metadata import Metadata


class ClickHouseMetadata(Metadata):
    def __init__(self, connection_info: ClickHouseConnectionInfo):
        super().__init__(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
            SELECT
                database AS table_schema,
                table AS table_name,
                name AS column_name,
                type AS data_type
            FROM
                system.columns
            WHERE
                database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema', 'pg_catalog');
            """
        response = loads(
            DataSource.clickhouse.get_connection(self.connection_info)
            .sql(sql)
            .to_pandas()
            .to_json(orient="records")
        )

        unique_tables = {}
        for row in response:
            # generate unique table name
            schema_table = self._format_compact_table_name(
                row["table_schema"], row["table_name"]
            )
            # init table if not exists
            if schema_table not in unique_tables:
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description="",
                    columns=[],
                    properties=TableProperties(
                        catalog=None,
                        schema=row["table_schema"],
                        table=row["table_name"],
                    ),
                    primaryKey="",
                )

            # table exists, and add column to the table
            unique_tables[schema_table].columns.append(
                Column(
                    name=row["column_name"],
                    type=self._transform_column_type(row["data_type"]),
                    notNull=False,
                    description="",
                    properties=None,
                )
            )
        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        return []

    def _format_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _transform_column_type(self, data_type):
        # lower case the data_type
        data_type = data_type.lower()

        # Map ClickHouse types to WrenEngineColumnType
        switcher = {
            "boolean": WrenEngineColumnType.BOOLEAN,
            "int8": WrenEngineColumnType.TINYINT,
            "uint8": WrenEngineColumnType.INT2,
            "int16": WrenEngineColumnType.INT2,
            "uint16": WrenEngineColumnType.INT2,
            "int32": WrenEngineColumnType.INT4,
            "uint32": WrenEngineColumnType.INT4,
            "int64": WrenEngineColumnType.INT8,
            "uint64": WrenEngineColumnType.INT8,
            "float32": WrenEngineColumnType.FLOAT4,
            "float64": WrenEngineColumnType.FLOAT8,
            "decimal": WrenEngineColumnType.DECIMAL,
            "date": WrenEngineColumnType.DATE,
            "datetime": WrenEngineColumnType.TIMESTAMP,
            "string": WrenEngineColumnType.VARCHAR,
            "fixedstring": WrenEngineColumnType.CHAR,
            "uuid": WrenEngineColumnType.UUID,
            "enum8": WrenEngineColumnType.STRING,  # Enums can be mapped to strings
            "enum16": WrenEngineColumnType.STRING,  # Enums can be mapped to strings
            "ipv4": WrenEngineColumnType.INET,
            "ipv6": WrenEngineColumnType.INET,
        }

        return switcher.get(data_type, WrenEngineColumnType.UNKNOWN)


def to_json(df):
    json_obj = loads(df.to_json(orient="split"))
    del json_obj["index"]
    return json_obj
