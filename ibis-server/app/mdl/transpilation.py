import base64
from collections import defaultdict

from orjson import orjson
from sqlglot import exp, parse_one
from sqlglot.optimizer.scope import build_scope

from app.model import UnprocessableEntityError
from app.model.data_source import DataSource


class Transpiler:
    def __init__(self, data_source: DataSource, manifest_str: str):
        self.data_source = data_source
        self.manifest = orjson.loads(base64.b64decode(manifest_str).decode("utf-8"))
        self.model_dict = self._build_model_dict(self.manifest["models"])

    def transpile(self, sql: str, write: str | None = None) -> str:
        ast = parse_one(sql, dialect=self.data_source.value)
        root = build_scope(ast)
        for scope in root.traverse():
            for alias, (node, source) in scope.selected_sources.items():
                if isinstance(source, exp.Table):
                    model = self.find_model(source)
                    if model is None:
                        raise TranspileError(f"Model not found: {source}")
                    source.replace(
                        exp.Table(
                            catalog=quote(self.manifest["catalog"]),
                            db=quote(self.manifest["schema"]),
                            this=quote(model["name"]),
                            alias=quote(alias),
                        )
                    )
        return ast.sql(dialect=write)

    @staticmethod
    def _build_model_dict(models):
        models_dict = defaultdict(lambda: defaultdict(list))
        for model in models:
            if table_ref := model.get("tableReference", None):
                catalog = table_ref.get("catalog", "")
                schema = table_ref.get("schema", "")
                models_dict[catalog][schema].append(model)
        return dict(models_dict)

    def find_model(self, source: exp.Table):
        catalog = source.catalog or ""
        schema = source.db or ""
        name = source.name
        if catalog not in self.model_dict or schema not in self.model_dict[catalog]:
            return None
        for model in self.model_dict[catalog][schema]:
            if model["tableReference"]["table"] == name:
                return model
        return None


def quote(s: str) -> str:
    return f'"{s}"'


class TranspileError(UnprocessableEntityError):
    def __init__(self, message: str):
        super().__init__(message)
