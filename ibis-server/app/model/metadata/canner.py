import re
from urllib.parse import urlparse

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from loguru import logger

from app.model import CannerConnectionInfo
from app.model.error import ErrorCode, WrenError
from app.model.metadata.dto import (
    Column,
    Constraint,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

CANNER_TYPE_MAPPING = {
    # String Types (ignore Binary and Spatial Types for now)
    "char": RustWrenEngineColumnType.CHAR,
    "varchar": RustWrenEngineColumnType.VARCHAR,
    "tinytext": RustWrenEngineColumnType.TEXT,
    "text": RustWrenEngineColumnType.TEXT,
    "mediumtext": RustWrenEngineColumnType.TEXT,
    "longtext": RustWrenEngineColumnType.TEXT,
    "enum": RustWrenEngineColumnType.VARCHAR,
    "set": RustWrenEngineColumnType.VARCHAR,
    # Numeric Types(https://dev.mysql.com/doc/refman/8.4/en/numeric-types.html)
    "bit": RustWrenEngineColumnType.TINYINT,
    "tinyint": RustWrenEngineColumnType.TINYINT,
    "smallint": RustWrenEngineColumnType.SMALLINT,
    "mediumint": RustWrenEngineColumnType.INTEGER,
    "int": RustWrenEngineColumnType.INTEGER,
    "integer": RustWrenEngineColumnType.INTEGER,
    "bigint": RustWrenEngineColumnType.BIGINT,
    # boolean
    "bool": RustWrenEngineColumnType.BOOL,
    "boolean": RustWrenEngineColumnType.BOOL,
    # Decimal
    "float": RustWrenEngineColumnType.FLOAT8,
    "double": RustWrenEngineColumnType.DOUBLE,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "numeric": RustWrenEngineColumnType.NUMERIC,
    # Date and Time Types(https://dev.mysql.com/doc/refman/8.4/en/date-and-time-types.html)
    "date": RustWrenEngineColumnType.DATE,
    "datetime": RustWrenEngineColumnType.TIMESTAMP,
    "timestamp": RustWrenEngineColumnType.TIMESTAMPTZ,
    # JSON Type
    "json": RustWrenEngineColumnType.JSON,
}


class CannerMetadata(Metadata):
    def __init__(self, connection_info: CannerConnectionInfo):
        super().__init__(connection_info)

        url_prefix = "https" if connection_info.enable_ssl else "http"
        url = f"{url_prefix}://{connection_info.host.get_secret_value()}/web/graphql"
        headers = {"Authorization": f"Token {connection_info.pat.get_secret_value()}"}
        self.client = Client(
            transport=AIOHTTPTransport(url=url, headers=headers),
            fetch_schema_from_transport=False,
        )

    def get_table_list(self) -> list[Table]:
        ws_sql_name = self._get_workspace_sql_name()
        metadata = self._get_metadata(self._get_workspace_id(ws_sql_name))
        return [self._build_table(data) for data in metadata]

    def get_constraints(self) -> list[Constraint]:
        return []

    def get_version(self) -> str:
        query = gql("""
            query SystemInfo {
                systemInfo {
                  version
                }
            }
        """)
        return self.client.execute(query)["systemInfo"]["version"]

    def _get_workspace_sql_name(self) -> str:
        if hasattr(self.connection_info, "connection_url"):
            return urlparse(
                self.connection_info.connection_url.get_secret_value()
            ).path.split("/")[-1]
        else:
            return self.connection_info.workspace.get_secret_value()

    def _get_workspace_id(self, ws_sql_name) -> str:
        query = gql("""
            query UserMe {
                userMe {
                    workspaces {
                        id
                        sqlName
                    }
                }
            }
        """)
        workspaces = self.client.execute(query)["userMe"]["workspaces"]
        try:
            return next(ws["id"] for ws in workspaces if ws["sqlName"] == ws_sql_name)
        except StopIteration:
            raise WrenError(
                ErrorCode.INVALID_CONNECTION_INFO, f"Workspace {ws_sql_name} not found"
            )

    def _get_metadata(self, workspace_id: str) -> dict:
        query = gql("""
            query WorkspaceDatasets($where: WorkspaceDatasetWhereInput!) {
              workspaceDatasets(where: $where) {
                ... on Table {
                  id
                  sqlName
                  displayName
                  columns {
                    originalColumn
                    dataMetadata {
                      metadata {
                        description
                      }
                    }
                  }
                  dataMetadata {
                    metadata {
                      description
                    }
                  }
                  properties
                  __typename
                }
                ... on MaterializedView {
                  id
                  sqlName
                  displayName
                  columns {
                    originalColumn
                    dataMetadata {
                      metadata {
                        description
                      }
                    }
                  }
                  dataMetadata {
                    metadata {
                      description
                    }
                  }
                  properties
                  __typename
                }
                ... on View {
                  id
                  sqlName
                  displayName
                  columns {
                    originalColumn
                    dataMetadata {
                      metadata {
                        description
                      }
                    }
                  }
                  dataMetadata {
                    metadata {
                      description
                    }
                  }
                  __typename
                }
                ... on Synonym {
                  id
                  sqlName
                  displayName
                  columns {
                    originalColumn
                    dataMetadata {
                      metadata {
                        description
                      }
                    }
                  }
                  dataMetadata {
                    metadata {
                      description
                    }
                  }
                  __typename
                }
              }
            }
        """)
        result = self.client.execute(
            query, variable_values={"where": {"workspaceId": workspace_id}}
        )
        return result["workspaceDatasets"]

    @classmethod
    def _build_table(cls, data: dict) -> Table:
        return Table(
            name=data["sqlName"],
            description=data["dataMetadata"]["metadata"]["description"],
            columns=cls._build_columns(data["columns"]),
            primaryKey="",
            properties=(
                TableProperties(
                    catalog="canner",
                    schema=data["properties"]["schema"],
                    table=data["properties"]["table"],
                )
                if data["properties"] and "schema" in data["properties"]
                else None
            ),
        )

    @classmethod
    def _build_columns(cls, columns: list[dict]) -> list[Column]:
        return [
            Column(
                name=column["originalColumn"]["name"],
                type=cls._transform_column_type(column["originalColumn"]["type"]),
                notNull=column["originalColumn"]["properties"].get(
                    "jdbc-nullable", False
                ),
                description=column["dataMetadata"]["metadata"]["description"],
                properties=column["originalColumn"]["properties"],
            )
            for column in columns
        ]

    @classmethod
    def _transform_column_type(self, data_type):
        # all possible types listed here: https://trino.io/docs/current/language/types.html
        # trim the (all characters) at the end of the data_type if exists
        data_type = re.sub(r"\(.*\)", "", data_type).strip()

        # Convert to lowercase for comparison
        normalized_type = data_type.lower()

        # Use the module-level mapping table
        mapped_type = CANNER_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )

        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown Canner data type: {data_type}")

        return mapped_type
