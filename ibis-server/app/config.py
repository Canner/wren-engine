import os

from dotenv import load_dotenv


class Config:
    def __init__(self):
        load_dotenv(override=True)
        self.wren_engine_endpoint = os.getenv('WREN_ENGINE_ENDPOINT')


config = Config()


def get_config() -> Config:
    return config
