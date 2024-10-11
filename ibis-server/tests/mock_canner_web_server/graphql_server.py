import base64
import hashlib
from typing import Optional

import orjson
import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter


@strawberry.type
class MDL:
    hash: str
    mdl: Optional[str] = None


@strawberry.type
class Query:
    @strawberry.field(name="getMDL", description="Follow Wren UI schema")
    def get_mdl(self, hash: str) -> MDL:
        pass


def dict_to_hash(data: dict[str, any]) -> str:
    return hashlib.sha256(orjson.dumps(data)).hexdigest()


def dict_to_base64_string(data: dict[str, any]) -> str:
    return base64.b64encode(orjson.dumps(data)).decode("utf-8")


schema = strawberry.Schema(query=Query)
router = GraphQLRouter(schema, path="/api/graphql")


app = FastAPI()
app.include_router(router)
