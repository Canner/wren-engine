from functools import wraps
from json import loads

import app.mdl.rewriter as rewriter
import app.routers.ibis as ibis
from app.logger import get_logger


def to_json(df) -> dict:
    json_obj = loads(df.to_json(orient='split'))
    del json_obj['index']
    json_obj['dtypes'] = df.dtypes.apply(lambda x: x.name).to_dict()
    return json_obj


def log_dto(f):
    logger = get_logger(ibis.__name__)

    @wraps(f)
    def wrapper(*args, dto):
        logger.debug(f'DTO: {dto}')
        return f(*args, dto)

    return wrapper


def log_rewritten(f):
    logger = get_logger(rewriter.__name__)

    @wraps(f)
    def wrapper(*args, **kwargs):
        rs = f(*args, **kwargs)
        logger.debug(f'Rewritten SQL: {rs}')
        return rs

    return wrapper
