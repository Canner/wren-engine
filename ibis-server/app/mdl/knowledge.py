from pathlib import Path

from app.model.data_source import DataSource
from app.model.error import ErrorCode, WrenError

KNOWLEDGE_RESOURCE_PATH = "resources/knowledge"


class Knowledge:
    def __init__(self, data_source: DataSource):
        self.data_source = data_source

    def get_text_to_sql_rule(self) -> str:
        rules_path = Path(f"{KNOWLEDGE_RESOURCE_PATH}/text_to_sql_rule.txt")
        if not rules_path.exists():
            raise WrenError(
                ErrorCode.GENERIC_INTERNAL_ERROR, "Text to SQL rule not found."
            )
        return rules_path.read_text()

    def get_sql_instructions(self) -> dict:
        instructions_path = Path(f"{KNOWLEDGE_RESOURCE_PATH}/instructions")
        if not instructions_path.exists():
            raise WrenError(
                ErrorCode.GENERIC_INTERNAL_ERROR, "SQL instructions path not found."
            )
        files = [f for f in instructions_path.iterdir() if f.is_file()]

        if self.data_source:
            dialect_file = Path(
                f"{KNOWLEDGE_RESOURCE_PATH}/dialects/{self.data_source.name}.txt"
            )
            if dialect_file.exists():
                files.append(dialect_file)
        return {file.stem: file.read_text() for file in files}

    def get_sql_correction_rule(self) -> str:
        rules_path = Path(f"{KNOWLEDGE_RESOURCE_PATH}/sql_correction_rule.txt")
        if not rules_path.exists():
            raise WrenError(
                ErrorCode.GENERIC_INTERNAL_ERROR, "SQL correction rule not found."
            )
        return rules_path.read_text()
