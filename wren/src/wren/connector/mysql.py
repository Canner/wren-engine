import ibis.expr.datatypes as dt
from ibis.expr.datatypes import Decimal
from ibis.expr.datatypes.core import UUID
from ibis.expr.types import Table

from wren.connector.base import IbisConnector
from wren.model.data_source import DataSource


class MySqlConnector(IbisConnector):
    def __init__(self, connection_info):
        super().__init__(DataSource.mysql, connection_info)

    def _handle_pyarrow_unsupported_type(self, ibis_table: Table, **kwargs) -> Table:
        result_table = ibis_table
        for name, dtype in ibis_table.schema().items():
            if isinstance(dtype, Decimal):
                result_table = self._round_decimal_columns(
                    result_table=result_table, col_name=name, **kwargs
                )
            elif isinstance(dtype, UUID):
                result_table = self._cast_uuid_columns(
                    result_table=result_table, col_name=name
                )
            elif isinstance(dtype, dt.JSON):
                result_table = result_table.mutate(
                    **{name: result_table[name].cast("string")}
                )
        return result_table


class DorisConnector(IbisConnector):
    def __init__(self, connection_info):
        super().__init__(DataSource.doris, connection_info)

    def _handle_pyarrow_unsupported_type(self, ibis_table: Table, **kwargs) -> Table:
        result_table = ibis_table
        for name, dtype in ibis_table.schema().items():
            if isinstance(dtype, Decimal):
                result_table = self._round_decimal_columns(
                    result_table=result_table, col_name=name, **kwargs
                )
            elif isinstance(dtype, UUID):
                result_table = self._cast_uuid_columns(
                    result_table=result_table, col_name=name
                )
            elif isinstance(dtype, dt.JSON):
                result_table = result_table.mutate(
                    **{name: result_table[name].cast("string")}
                )
        return result_table


def create_connector(data_source: DataSource, connection_info):
    if data_source == DataSource.doris:
        return DorisConnector(connection_info)
    return MySqlConnector(connection_info)
