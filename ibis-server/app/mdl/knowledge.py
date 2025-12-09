from pathlib import Path

from app.model.data_source import DataSource

KNOWLEDGE_RESOURCE_PATH = "resources/knowledge"


class Knowledge:
    def __init__(self, data_source: DataSource):
        self.data_source = data_source

    def get_text_to_sql_rule(self) -> str:
        rules_path = Path(f"{KNOWLEDGE_RESOURCE_PATH}/text_to_sql_rules.txt")
        return rules_path.read_text()

    def get_sql_instructions(self) -> dict:
        instructions_path = Path(f"{KNOWLEDGE_RESOURCE_PATH}/instructions")
        files = [f for f in instructions_path.iterdir() if f.is_file()]
        return {file.stem: file.read_text() for file in files}
