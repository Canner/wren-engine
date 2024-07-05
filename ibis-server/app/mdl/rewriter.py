import httpx
import orjson
import sqlglot

from app.config import get_config
from app.logger import get_logger
from app.model.data_source import DataSource

wren_engine_endpoint = get_config().wren_engine_endpoint

logger = get_logger("app.mdl.rewriter")


class Rewriter:
    def __init__(self, manifest_str: str, data_source: DataSource = None):
        self.manifest_str = manifest_str
        self.data_source = data_source

    def rewrite(self, sql: str) -> str:
        try:
            r = httpx.request(
                method="GET",
                url=f"{wren_engine_endpoint}/v2/mdl/dry-plan",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                content=orjson.dumps({"manifestStr": self.manifest_str, "sql": sql}),
            )
            rewritten_sql = (
                r.text if r.status_code == httpx.codes.OK else r.raise_for_status()
            )
            logger.debug("Rewritten SQL: %s", rewritten_sql)
            return (
                rewritten_sql
                if self.data_source is None
                else self.transpile(rewritten_sql)
            )
        except httpx.ConnectError as e:
            raise ConnectionError(f"Can not connect to Wren Engine: {e}")

    def transpile(self, rewritten_sql: str) -> str:
        transpiled_sql = sqlglot.transpile(
            rewritten_sql, read="trino", write=self.data_source.name
        )[0]
        logger.debug("Translated SQL: %s", transpiled_sql)
        return transpiled_sql
