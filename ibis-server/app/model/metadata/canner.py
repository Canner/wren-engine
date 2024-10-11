from urllib.parse import urlparse

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from app.model import CannerConnectionInfo
from app.model.metadata.dto import (
    Column,
    Constraint,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata


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
            raise ValueError(f"Workspace {ws_sql_name} not found")

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
                type=column["originalColumn"]["type"],
                notNull=column["originalColumn"]["properties"].get(
                    "jdbc-nullable", False
                ),
                description=column["dataMetadata"]["metadata"]["description"],
                properties=column["originalColumn"]["properties"],
            )
            for column in columns
        ]
