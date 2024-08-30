import calendar
import datetime
import decimal

import orjson
import pandas as pd


def to_json(df: pd.DataFrame) -> dict:
    return _to_json_obj(df)


def _to_json_obj(df: pd.DataFrame) -> dict:
    data = df.to_dict(orient="split", index=False)

    def default(d):
        if pd.isnull(d):
            return None
        if isinstance(d, decimal.Decimal):
            return float(d)
        elif isinstance(d, pd.Timestamp):
            return d.value // 10**6
        elif isinstance(d, datetime.datetime):
            return int(d.timestamp())
        elif isinstance(d, datetime.date):
            return calendar.timegm(d.timetuple()) * 1000
        else:
            raise d

    json_obj = orjson.loads(
        orjson.dumps(
            data,
            option=orjson.OPT_SERIALIZE_NUMPY
            | orjson.OPT_PASSTHROUGH_DATETIME
            | orjson.OPT_SERIALIZE_UUID,
            default=default,
        )
    )
    json_obj["dtypes"] = df.dtypes.astype(str).to_dict()
    return json_obj
