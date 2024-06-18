from wren_engine.model.connector import ConnectionInfo
from wren_engine.model.metadata.dto import Table, Constraint


class Metadata:
    def __init__(self, connection_info: ConnectionInfo):
        self.connection_info = connection_info

    def get_table_list(self) -> list[Table]:
        raise NotImplementedError

    def get_constraints(self) -> list[Constraint]:
        raise NotImplementedError
