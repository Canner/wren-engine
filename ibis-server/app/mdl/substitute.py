from collections import defaultdict

from opentelemetry import trace
from sqlglot import exp, parse_one
from sqlglot.optimizer.scope import build_scope

from app.model.data_source import DataSource
from app.model.error import ErrorCode, ErrorPhase, WrenError
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
            for alias, (_node, source) in scope.selected_sources.items():
                if not isinstance(source, exp.Table):
                    continue

                key, sensitive_model = self._find_model(source)
                _, case_insensitive_model = self._find_model(
                    source, case_sensitive=False
                )
                model = sensitive_model or case_insensitive_model

                # if model name is ambiguous, raise an error
                duplicate_keys = get_case_insensitive_duplicate_keys(self.model_dict)
                if model is not None and key.lower() in duplicate_keys:
                    raise WrenError(
                        ErrorCode.GENERIC_USER_ERROR,
                        f"Ambiguous model: found multiple matches for {source}",
                        phase=ErrorPhase.SQL_SUBSTITUTE,
                    )

                if model is None:
                    raise WrenError(
                        ErrorCode.NOT_FOUND,
                        f"Model not found: {source}",
                        phase=ErrorPhase.SQL_SUBSTITUTE,
                    )

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
        return {
            ModelSubstitute._build_key(model): model
            for model in models
            if "tableReference" in model
        }

    @staticmethod
    def _build_case_insensitive_model_dict(models) -> dict:
        return {
            ModelSubstitute._build_key(model).lower(): model
            for model in models
            if "tableReference" in model
        }

    def _build_key(model):
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

        # catalog and schema is not None and not empty string
        if catalog and schema:
            key = f"{catalog}.{schema}.{table}"
        # schema is not None and not empty string
        elif schema:
            key = f"{schema}.{table}"
        else:
            key = f"{table}"

        # Determine if case insensitive search is needed
        if case_sensitive:
            return key, self.model_dict.get(key, None)
        else:
            return key.lower(), self.model_dict_case_insensitive.get(key.lower(), None)


def quote(s: str) -> str:
    return f'"{s}"'


def get_case_insensitive_duplicate_keys(d):
    key_map = defaultdict(list)
    for k in d:
        key_map[k.lower()].append(k.lower())

    duplicates = [key for keys in key_map.values() if len(keys) > 1 for key in keys]
    return duplicates
