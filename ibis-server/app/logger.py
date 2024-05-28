import logging

from app.config import get_config

logging.basicConfig(level=get_config().log_level)


def get_logger(name):
    return logging.getLogger(name)
