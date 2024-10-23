from ibis.backends.sql import datatypes


class BigQueryType(datatypes.BigQueryType):
    default_interval_precision = "s"


datatypes.BigQueryType = BigQueryType
