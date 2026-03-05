from sqlglot import exp
from sqlglot.dialects import Doris as OriginalDoris


class Doris(OriginalDoris):
    class Generator(OriginalDoris.Generator):
        TYPE_MAPPING = {
            **OriginalDoris.Generator.TYPE_MAPPING,
            exp.DataType.Type.VARBINARY: "BINARY",
        }
