from app.model import ConnectionInfo
from app.model.error import ErrorCode, WrenError
from app.model.metadata.dto import Catalog, Constraint, FilterInfo, Table


class Metadata:
    def __init__(self, connection_info: ConnectionInfo):
        self.connection_info = connection_info

    def get_table_list(
        self, filter_info: FilterInfo | None = None, limit: int | None = None
    ) -> list[Table]:
        raise WrenError(ErrorCode.NOT_IMPLEMENTED, "get_table_list not implemented")

    def get_constraints(self) -> list[Constraint]:
        raise WrenError(ErrorCode.NOT_IMPLEMENTED, "get_constraints not implemented")

    def get_version(self) -> str:
        raise WrenError(ErrorCode.NOT_IMPLEMENTED, "get_version not implemented")

    def get_schema_list(
        self, filter_info: FilterInfo | None = None, limit: int | None = None
    ) -> list[Catalog]:
        raise WrenError(ErrorCode.NOT_IMPLEMENTED, "get_schema_list not implemented")
