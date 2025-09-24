import re
from abc import ABC, abstractmethod

from tests.model import Function


class SqlTestGenerator:
    def __init__(self, dialect: str):
        self.dialect = dialect
        self._generator = self._get_generator()

    def generate_sql(self, function: Function) -> str | None:
        if function.function_type == "aggregate":
            return self._generator.generate_aggregate_sql(function)
        if function.function_type == "scalar":
            return self._generator.generate_scalar_sql(function)
        if function.function_type == "window":
            return self._generator.generate_window_sql(function)
        raise NotImplementedError(
            f"Unsupported function type: {function.function_type}"
        )

    def _get_generator(self):
        if self.dialect == "bigquery":
            return BigQuerySqlGenerator()
        if self.dialect == "postgres":
            return PostgresSqlGenerator()
        if self.dialect == "mysql":
            return MySqlGenerator()
        if self.dialect == "mssql":
            return MSSqlGenerator()
        if self.dialect == "clickhouse":
            return ClickhouseSqlGenerator()
        if self.dialect == "trino":
            return TrinoSqlGenerator()
        raise NotImplementedError(f"Unsupported dialect: {self.dialect}")

    @staticmethod
    def map_sample_parm_by_type(p_type: str) -> str:
        p_type = p_type.lower()
        if p_type in {"int", "integer", "bigint", "smallint"}:
            return "1"
        elif p_type in {"numeric", "decimal", "double precision", "float", "real"}:
            return "1.0"
        elif p_type in {"text", "varchar", "char", "string"}:
            return "'test'"
        elif p_type in {"boolean", "bool"}:
            return "TRUE"
        elif p_type in {"date"}:
            return "cast('2024-01-01' as date)"
        elif p_type in {
            "timestamp",
            "timestamp without time zone",
            "timestamp with time zone",
        }:
            return "cast('2024-01-01 00:00:00' as timestamp)"
        elif p_type in {"time", "time without time zone", "time with time zone"}:
            return "cast('12:00:00' as time)"
        elif p_type == "uuid":
            return "'123e4567-e89b-12d3-a456-426614174000'"
        elif p_type.startswith("array"):
            inner_type = (
                re.match(r"array<(.+)>", p_type).group(1)
                if p_type.startswith("array<") and p_type.endswith(">")
                else "int"
            )
            element = SqlTestGenerator.map_sample_parm_by_type(inner_type)
            return f"ARRAY[{element}, {element}]"
        elif p_type in {"json", "jsonb"}:
            return '\'{"key": "value"}\''
        elif p_type == "bytea":
            return "'\\xc3a4'"
        elif p_type == "interval":
            return "INTERVAL 1 DAY"
        elif p_type == "granularity":
            return "DAY"
        else:
            return "NULL"


class SqlGenerator(ABC):
    @abstractmethod
    def generate_aggregate_sql(self, function: Function) -> str:
        raise NotImplementedError

    @abstractmethod
    def generate_scalar_sql(self, function: Function) -> str:
        raise NotImplementedError

    @abstractmethod
    def generate_window_sql(self, function: Function) -> str:
        raise NotImplementedError

    @staticmethod
    def generate_sample_args(param_types: list[str]) -> list[str]:
        return [
            SqlTestGenerator.map_sample_parm_by_type(p_type) for p_type in param_types
        ]


class PostgresSqlGenerator(SqlGenerator):
    def generate_aggregate_sql(self, function: Function) -> str:
        sample_args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(sample_args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_scalar_sql(self, function: Function) -> str:
        args = self.generate_sample_args(function.param_types)
        if function.name in ("convert_from", "convert_to"):
            args[1] = "'UTF8'"
        if function.name == "extract":
            args[0] = "'day'"
        if function.name in ("json_array_length", "jsonb_array_length"):
            args[0] = '\'[{"key": "value"}]\''
        if function.name in ("json_extract_path", "jsonb_extract_path"):
            args[1] = "'key'"
        if function.name == "to_number":
            args = ["'123'", "'999'"]
        formatted_args = ", ".join(args)
        return f"SELECT {function.name}({formatted_args})"


class BigQuerySqlGenerator(SqlGenerator):
    def generate_aggregate_sql(self, function: Function) -> str:
        args = self.generate_sample_args(function.param_types)
        if function.name == "array_agg":
            args[0] = "x"
            table = "(SELECT 1 AS x UNION ALL SELECT 2)"
        else:
            table = "(SELECT 1)"
        formatted_args = ", ".join(args)
        return f"SELECT {function.name}({formatted_args}) FROM {table} AS t(x)"

    def generate_scalar_sql(self, function: Function) -> str:
        args = self.generate_sample_args(function.param_types)
        if function.name in (
            "json_query",
            "json_value",
            "json_query_array",
            "json_value_array",
        ):
            args[1] = "'$'"
        if function.name in ("format_timestamp", "format_date"):
            args[0] = "'%c'"
        if function.name == "parse_date":
            args[0] = "'%F'"
            args[1] = "'2000-12-30'"
        formatted_args = ", ".join(args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_window_sql(self, function: Function) -> str:
        return f"""
            SELECT
                {function.name}() OVER (ORDER BY id) AS {function.name.lower()} 
            FROM (
                SELECT 1 AS id, 'A' AS category UNION ALL
                SELECT 2 AS id, 'B' AS category UNION ALL
                SELECT 3 AS id, 'A' AS category UNION ALL
                SELECT 4 AS id, 'B' AS category UNION ALL
                SELECT 5 AS id, 'A' AS category
            ) AS t
        """


class MySqlGenerator(SqlGenerator):
    def generate_aggregate_sql(self, function: Function) -> str:
        sample_args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(sample_args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_scalar_sql(self, function: Function) -> str:
        args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(args)
        return f"SELECT {function.name}({formatted_args})"


class MSSqlGenerator(SqlGenerator):
    def generate_aggregate_sql(self, function: Function) -> str:
        sample_args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(sample_args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_scalar_sql(self, function: Function) -> str:
        args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_window_sql(self, function: Function) -> str:
        return f"""
            SELECT
                {function.name}() OVER (ORDER BY id) AS {function.name.lower()} 
            FROM (
                SELECT 1 AS id, 'A' AS category UNION ALL
                SELECT 2 AS id, 'B' AS category UNION ALL
                SELECT 3 AS id, 'A' AS category UNION ALL
                SELECT 4 AS id, 'B' AS category UNION ALL
                SELECT 5 AS id, 'A' AS category
            ) AS t
        """


class ClickhouseSqlGenerator(SqlGenerator):
    def generate_aggregate_sql(self, function: Function) -> str:
        sample_args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(sample_args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_scalar_sql(self, function: Function) -> str:
        args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_window_sql(self, function: Function) -> str:
        return f"""
            SELECT
                {function.name}() OVER (ORDER BY id) AS {function.name.lower()} 
            FROM (
                SELECT 1 AS id, 'A' AS category UNION ALL
                SELECT 2 AS id, 'B' AS category UNION ALL
                SELECT 3 AS id, 'A' AS category UNION ALL
                SELECT 4 AS id, 'B' AS category UNION ALL
                SELECT 5 AS id, 'A' AS category
            ) AS t
        """


class TrinoSqlGenerator(SqlGenerator):
    def generate_aggregate_sql(self, function: Function) -> str:
        sample_args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(sample_args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_scalar_sql(self, function: Function) -> str:
        args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(args)
        return f"SELECT {function.name}({formatted_args})"

    def generate_window_sql(self, function: Function) -> str:
        return f"""
            SELECT
                {function.name}() OVER (ORDER BY id) AS {function.name.lower()} 
            FROM (
                SELECT 1 AS id, 'A' AS category UNION ALL
                SELECT 2 AS id, 'B' AS category UNION ALL
                SELECT 3 AS id, 'A' AS category UNION ALL
                SELECT 4 AS id, 'B' AS category UNION ALL
                SELECT 5 AS id, 'A' AS category
            ) AS t
        """
