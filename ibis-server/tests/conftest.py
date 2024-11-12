import os


def file_path(path: str) -> str:
    return os.path.join(os.path.dirname(__file__), path)


DATAFUSION_FUNCTION_COUNT = 269
