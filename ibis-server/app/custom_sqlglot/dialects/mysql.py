from sqlglot import exp
from sqlglot.dialects import MySQL as OriginalMySQL


class MySQL(OriginalMySQL):
    class Generator(OriginalMySQL.Generator):
        TYPE_MAPPING = {
            **OriginalMySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.VARBINARY: "BINARY",
        }
