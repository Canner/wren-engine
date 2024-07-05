import os

from dotenv import load_dotenv


class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        load_dotenv(override=True)
        self.wren_engine_endpoint = os.getenv("WREN_ENGINE_ENDPOINT")
        self.validate_wren_engine_endpoint(self.wren_engine_endpoint)
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

    @staticmethod
    def validate_wren_engine_endpoint(endpoint):
        if endpoint is None:
            raise ValueError("WREN_ENGINE_ENDPOINT is not set")


def get_config() -> Config:
    return Config()
