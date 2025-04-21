from opentelemetry import trace
from sqlglot import exp, parse_one
from sqlglot.optimizer.scope import build_scope

from app.model import UnprocessableEntityError
from app.model.data_source import DataSource
from app.util import base64_to_dict

tracer = trace.get_tracer(__name__)


class ModelSubstitute:
    def __init__(self, data_source: DataSource, manifest_str: str, headers=None):
        self.data_source = data_source
        self.manifest = base64_to_dict(manifest_str)
        self.model_dict = self._build_model_dict(self.manifest["models"])
        self.model_dict_case_insensitive = self._build_case_insensitive_model_dict(
            self.manifest["models"]
        )
        self.headers = dict(headers) if headers else None

    @tracer.start_as_current_span("substitute", kind=trace.SpanKind.INTERNAL)
    def substitute(self, sql: str, write: str | None = None) -> str:
        ast = parse_one(sql, dialect=self.data_source.value)
        root = build_scope(ast)

        for scope in root.traverse():
            for alias, (node, source) in scope.selected_sources.items():
                if not isinstance(source, exp.Table):
                    continue

                model = self._find_model(source) or self._find_model(
                    source, case_sensitive=False
                )
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

            # fully qualified  catalog.schema.table
            if table_ref.get("catalog") and table_ref.get("schema"):
                return f"{table_ref.get('catalog', '')}.{table_ref.get('schema', '')}.{table_ref.get('table', '')}"
            # schema.table
            elif table_ref.get("schema"):
                return f"{table_ref.get('schema', '')}.{table_ref.get('table', '')}"
            # table
            else:
                return table_ref.get("table", "")

        return {key(model): model for model in models if "tableReference" in model}

    @staticmethod
    def _build_case_insensitive_model_dict(models) -> dict:
        def key(model):
            table_ref = model["tableReference"]

            # fully qualified catalog.schema.table
            if table_ref.get("catalog") and table_ref.get("schema"):
                return f"{table_ref.get('catalog', '')}.{table_ref.get('schema', '')}.{table_ref.get('table', '')}".lower()
            # schema.table
            elif table_ref.get("schema"):
                return f"{table_ref.get('schema', '')}.{table_ref.get('table', '')}".lower()
            # table
            else:
                return table_ref.get("table", "").lower()

        return {key(model): model for model in models if "tableReference" in model}

    def _find_model(self, source: exp.Table, case_sensitive=True) -> dict | None:
        # Determine catalog
        if source.catalog:
            catalog = source.catalog
        else:
            catalog = self.headers.get("x-user-catalog", "") if self.headers else ""

        # Determine schema
        if source.db:
            schema = source.db
        else:
            schema = self.headers.get("x-user-schema", "") if self.headers else ""

        table = source.name

        # Determine if case insensitive search is needed
        if case_sensitive:
            model_dict = self.model_dict
            # catalog and schema is not None and not empty string
            if catalog and schema:
                return model_dict.get(f"{catalog}.{schema}.{table}", None)
            # catalog is not None and not empty string
            elif schema:
                return model_dict.get(f"{schema}.{table}", None)
            else:
                return model_dict.get(f"{table}", None)

        else:
            model_dict = self.model_dict_case_insensitive
            # catalog and schema is not None and not empty string
            if catalog and schema:
                return model_dict.get(f"{catalog}.{schema}.{table}".lower(), None)
            # catalog is not None and not empty string
            elif schema:
                return model_dict.get(f"{schema}.{table}".lower(), None)
            else:
                return model_dict.get(f"{table}".lower(), None)


def quote(s: str) -> str:
    return f'"{s}"'


class SubstituteError(UnprocessableEntityError):
    def __init__(self, message: str):
        super().__init__(message)
