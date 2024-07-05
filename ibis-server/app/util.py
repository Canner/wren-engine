import calendar
import datetime
import decimal

import orjson
import pandas as pd


def to_json(df: pd.DataFrame, column_dtypes: dict[str, str] | None) -> dict:
    if column_dtypes:
        _to_specific_types(df, column_dtypes)
    return _to_json_obj(df)


def _to_specific_types(df: pd.DataFrame, column_dtypes: dict[str, str]):
    for column, dtype in column_dtypes.items():
        if dtype == "datetime64":
            df[column] = _to_datetime_and_format(df[column])
        else:
            df[column] = df[column].astype(dtype)


def _to_datetime_and_format(series: pd.Series) -> pd.Series:
    series = pd.to_datetime(series, errors="coerce")
    return series.apply(
        lambda d: d.strftime(
            "%Y-%m-%d %H:%M:%S.%f" + (" %Z" if series.dt.tz is not None else "")
        )
        if not pd.isnull(d)
        else d
    )


def _to_json_obj(df: pd.DataFrame) -> dict:
    data = df.to_dict(orient="split", index=False)

    def default(d):
        match d:
            case decimal.Decimal():
                return float(d)
            case pd.Timestamp():
                return d.value // 10**6
            case datetime.datetime():
                return int(d.timestamp())
            case datetime.date():
                return calendar.timegm(d.timetuple()) * 1000
            case _:
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
