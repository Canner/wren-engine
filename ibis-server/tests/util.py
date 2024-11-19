import csv
from typing import Optional

from pydantic import BaseModel


class Function(BaseModel):
    function_type: str
    name: str
    return_type: str
    param_names: list[str] | None
    param_types: list[str] | None
    description: str


class FunctionCsvParser:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def parse(self) -> list[Function]:
        with open(self.file_path, encoding="utf-8") as csvfile:
            return [
                Function(
                    function_type=row["function_type"],
                    name=row["name"],
                    return_type=row["return_type"],
                    param_names=self._split_param(row["param_names"]),
                    param_types=self._split_param(row["param_types"]),
                    description=row["description"],
                )
                for row in csv.DictReader(csvfile)
            ]

    @staticmethod
    def _split_param(param: str) -> list[str]:
        return param.split(",") if param else []


class SqlTestGenerator:
    def __init__(self):
        pass

    def generate_sql(self, function: Function) -> Optional[str]:
        if function.function_type == "scalar":
            return self.generate_scalar_sql(function)
        elif function.function_type == "aggregate":
            return self.generate_aggregate_sql(function)
        elif function.function_type == "window":
            return self.generate_window_sql(function)
        else:
            raise NotImplementedError(
                f"Unsupported function type: {function.function_type}"
            )

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

    def generate_aggregate_sql(self, function: Function) -> str:
        sample_args = self.generate_sample_args(function.param_types)
        formatted_args = ", ".join(sample_args)
        sql = f"SELECT {function.name}({formatted_args})"
        return sql

    def generate_window_sql(self, function: Function) -> str:
        # TODO: Implement window function generation
        return ""

    def generate_sample_args(self, param_types: list[str]) -> list[str]:
        return [self.map_param_type_to_sample(p_type) for p_type in param_types]

    @staticmethod
    def map_param_type_to_sample(p_type: str) -> str:
        p_type = p_type.lower()
        if p_type in {"int", "integer", "bigint", "smallint"}:
            return "1"
        elif p_type in {"numeric", "decimal", "double precision", "float", "real"}:
            return "1.0"
        elif p_type in {"text", "varchar", "char"}:
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
            inner_type = p_type[p_type.find("<") + 1 : p_type.find(">")].strip()
            sample_inner = SqlTestGenerator.map_param_type_to_sample(inner_type)
            return f"ARRAY[{sample_inner}, {sample_inner}, {sample_inner}]"
        elif p_type in {"json", "jsonb"}:
            return '\'{"key": "value"}\''
        elif p_type == "bytea":
            return "'\\xc3a4'"
        elif p_type == "interval":
            return "'1 day'"
        else:
            return "NULL"
