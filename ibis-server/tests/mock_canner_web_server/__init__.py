import time
from multiprocessing import Process

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError
from loguru import logger
from orjson import orjson

from tests.mock_canner_web_server import graphql_server


class MockWebServer:
    def __init__(self):
        self.remote_proc = Process(target=graphql_server.start, args=(), daemon=True)
        self.remote_proc.start()
        self.url = "http://localhost:3000/api/graphql"
        self.client = Client(
            transport=AIOHTTPTransport(url=self.url), fetch_schema_from_transport=True
        )
        self._wait_server_ready()

    def register_mdl(self, mdl: dict[str, any]) -> str:
        mutation = gql(
            """
            mutation AddMDL($mdl: String!) {
                addMDL(mdl: $mdl) {
                    hash
                }
            }
        """
        )
        result = self.client.execute(
            mutation, variable_values={"mdl": orjson.dumps(mdl).decode("utf-8")}
        )
        return result["addMDL"]["hash"]

    def close(self):
        self.remote_proc.terminate()

    def _wait_server_ready(self):
        retry_interval = 0.5
        query = gql("""
            query {
                __typename
            }
        """)

        for _ in range(30):
            try:
                self.client.execute(query)
                return
            except TransportQueryError:
                time.sleep(retry_interval)
            except Exception as e:
                logger.warning(f"Unexpected error while waiting for server: {e}")
                time.sleep(retry_interval)

        raise TimeoutError("Server did not become ready in time")
