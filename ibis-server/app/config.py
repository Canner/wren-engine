import logging
import os
import sys

from dotenv import load_dotenv
from loguru import logger

logging.getLogger("uvicorn.error").disabled = True

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<yellow>[{extra[request_id]}]</yellow> | "
    "<level>{level: <8}</level> | "
    "<cyan>{module}.{function}:{line}</cyan> - <level>{message}</level>"
)


class Config:
    def __init__(self):
        load_dotenv(override=True)
        self.wren_engine_endpoint = os.getenv("WREN_ENGINE_ENDPOINT")
        self.validate_wren_engine_endpoint(self.wren_engine_endpoint)
        self.diagnose = False
        self.init_logger()

    @staticmethod
    def validate_wren_engine_endpoint(endpoint):
        if endpoint is None:
            raise ValueError("WREN_ENGINE_ENDPOINT is not set")

    @staticmethod
    def init_logger():
        logger.remove()
        logger.add(
            sys.stderr,
            format=logger_format,
            backtrace=True,
            diagnose=False,
            enqueue=True,
        )

    @staticmethod
    def logger_diagnose():
        logger.remove()
        logger.add(
            sys.stderr,
            format=logger_format,
            backtrace=True,
            diagnose=True,
            enqueue=True,
        )

    def update(self, diagnose: bool):
        self.diagnose = diagnose
        if diagnose:
            self.logger_diagnose()
        else:
            self.init_logger()


config = Config()


def get_config() -> Config:
    return config
