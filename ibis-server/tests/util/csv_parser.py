import csv

from tests.model import Function


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
