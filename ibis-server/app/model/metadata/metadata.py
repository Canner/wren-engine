from abc import ABC, abstractmethod

from app.model import ConnectionInfo
from app.model.metadata.dto import Constraint, Table


class Metadata(ABC):
    def __init__(self, connection_info: ConnectionInfo):
        self.connection_info = connection_info

    @abstractmethod
    def get_table_list(self) -> list[Table]:
        pass

    @abstractmethod
    def get_constraints(self) -> list[Constraint]:
        pass

    @abstractmethod
    def get_version(self) -> str:
        pass
