import os
import pathlib

import pytest

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.athena

base_url = "/v3/connector/athena"

function_list_path = file_path("../resources/function_list")


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="session")
def connection_info_oidc():
    """Use web identity token (OIDC → AssumeRoleWithWebIdentity) authentication."""
    token_path = os.getenv("TEST_ATHENA_WEB_IDENTITY_TOKEN_PATH")
    role_arn = os.getenv("TEST_ATHENA_ROLE_ARN")

    if not token_path or not role_arn:
        pytest.skip("Skipping OIDC test: web identity token or role ARN not set")

    with open(token_path, encoding="utf-8") as f:
        web_identity_token = f.read().strip()

    return {
        "s3_staging_dir": os.getenv("TEST_ATHENA_S3_STAGING_DIR"),
        "region_name": os.getenv("TEST_ATHENA_REGION_NAME", "ap-northeast-1"),
        "schema_name": "test",
        "role_arn": role_arn,
        "role_session_name": "pytest-session",
        "web_identity_token": web_identity_token,
    }


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    yield
    config.set_remote_function_list_path(None)
