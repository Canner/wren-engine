import re

from testcontainers.core.config import testcontainers_config as c
from testcontainers.core.generic import DbContainer
from testcontainers.core.waiting_utils import wait_container_is_ready, wait_for_logs
from trino.dbapi import connect


# Reference: https://github.com/testcontainers/testcontainers-python/pull/152
class TrinoContainer(DbContainer):
    def __init__(
        self,
        image="trinodb/trino:latest",
        port: int = 8080,
        **kwargs,
    ):
        super().__init__(image=image, **kwargs)
        self.port = port
        self.with_exposed_ports(self.port)

    @wait_container_is_ready()
    def _connect(self) -> None:
        wait_for_logs(
            self,
            re.compile(".*======== SERVER STARTED ========.*", re.MULTILINE).search,
            c.max_tries,
            c.sleep_time,
        )
        conn = connect(
            host=self.get_container_host_ip(),
            port=self.get_exposed_port(self.port),
            user="test",
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        conn.close()

    def get_connection_url(self):
        return f"trino://{self.get_container_host_ip()}:{self.port}"

    def _configure(self):
        pass
