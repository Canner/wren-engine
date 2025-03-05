from opentelemetry import trace
from sqlglot import exp, parse_one
from sqlglot.optimizer.scope import build_scope

from app.model import UnprocessableEntityError
from app.model.data_source import DataSource
from app.util import base64_to_dict

tracer = trace.get_tracer(__name__)


class ModelSubstitute:
    def __init__(self, data_source: DataSource, manifest_str: str):
        self.data_source = data_source
        self.manifest = base64_to_dict(manifest_str)
        self.model_dict = self._build_model_dict(self.manifest["models"])

    @tracer.start_as_current_span("substitute", kind=trace.SpanKind.INTERNAL)
    def substitute(self, sql: str, write: str | None = None) -> str:
        ast = parse_one(sql, dialect=self.data_source.value)
        root = build_scope(ast)
        for scope in root.traverse():
            for alias, (node, source) in scope.selected_sources.items():
                if isinstance(source, exp.Table):
                    model = self._find_model(source)
                    if model is None:
                        raise SubstituteError(f"Model not found: {source}")
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
    def _build_model_dict(models) -> dict:
        def key(model):
            table_ref = model["tableReference"]
            return f"{table_ref.get('catalog', '')}.{table_ref.get('schema', '')}.{table_ref.get('table', '')}"

        return {key(model): model for model in models if "tableReference" in model}

    def _find_model(self, source: exp.Table) -> dict | None:
        catalog = source.catalog or ""
        schema = source.db or ""
        table = source.name
        return self.model_dict.get(f"{catalog}.{schema}.{table}", None)


def quote(s: str) -> str:
    return f'"{s}"'


class SubstituteError(UnprocessableEntityError):
    def __init__(self, message: str):
        super().__init__(message)
