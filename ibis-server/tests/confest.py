import os


def file_path(path: str) -> str:
    return os.path.join(os.path.dirname(__file__), path)
