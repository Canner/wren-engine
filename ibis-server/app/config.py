import os

from dotenv import load_dotenv


class Config:
    def __init__(self):
        load_dotenv(override=True)
        self.wren_engine_endpoint = os.getenv('WREN_ENGINE_ENDPOINT')
        self.validate_wren_engine_endpoint(self.wren_engine_endpoint)

    @staticmethod
    def validate_wren_engine_endpoint(endpoint):
        if endpoint is None:
            raise ValueError('WREN_ENGINE_ENDPOINT is not set')


config = Config()


def get_config() -> Config:
    return config
