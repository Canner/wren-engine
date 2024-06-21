from app.model import ConnectionInfo
from app.model.metadata.dto import Constraint, Table


class Metadata:
    def __init__(self, connection_info: ConnectionInfo):
        self.connection_info = connection_info

    def get_table_list(self) -> list[Table]:
        raise NotImplementedError

    def get_constraints(self) -> list[Constraint]:
        raise NotImplementedError
