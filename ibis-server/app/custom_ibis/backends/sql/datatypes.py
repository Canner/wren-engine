from ibis.backends.sql import datatypes


class BigQueryType(datatypes.BigQueryType):
    # It's a workaround for the issue of ibs BQ connector not supporting interval precision.
    # Set `h` to avoid ibis try to cast arrow interval to duration, which leads to an casting error of pyarrow.
    # See: https://github.com/ibis-project/ibis/blob/main/ibis/formats/pyarrow.py#L182
    default_interval_precision = "h"


datatypes.BigQueryType = BigQueryType
