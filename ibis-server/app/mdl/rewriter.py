import httpx
import orjson

from app.config import get_config
from app.model.mdl import Manifest

wren_engine_endpoint = get_config().wren_engine_endpoint


class Rewriter:
    def __init__(self, manifest: Manifest):
        self.manifest = manifest

    def rewrite(self, sql) -> str:
        r = httpx.request(
            method='GET',
            url=f'{wren_engine_endpoint}/v1/mdl/dry-plan',
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            content=orjson.dumps({
                'manifest': self.manifest.model_dump(by_alias=True),
                'sql': sql,
                'isModelingOnly': True}))

        return r.text if r.status_code == httpx.codes.OK else r.raise_for_status()
