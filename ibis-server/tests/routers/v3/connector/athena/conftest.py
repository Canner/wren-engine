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
def connection_info():
    return {
        "s3_staging_dir": os.getenv("TEST_ATHENA_S3_STAGING_DIR"),
        "aws_access_key_id": os.getenv("TEST_ATHENA_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("TEST_ATHENA_AWS_SECRET_ACCESS_KEY"),
        "region_name": os.getenv("TEST_ATHENA_REGION_NAME", "ap-northeast-1"),
        "schema_name": "test",
    }


@pytest.fixture(scope="session")
def connection_info_default_credential_chain():
    # Use default authentication (e.g., from environment variables, shared config file, or EC2 instance profile)
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        pytest.skip(
            "Skipping default credential chain test: AWS credentials not set in environment"
        )
    return {
        "s3_staging_dir": os.getenv("TEST_ATHENA_S3_STAGING_DIR"),
        "region_name": os.getenv("TEST_ATHENA_REGION_NAME", "ap-northeast-1"),
        "schema_name": "test",
    }


@pytest.fixture(scope="session")
def connection_info_oidc():
    web_identity_token = os.getenv("TEST_ATHENA_WEB_IDENTITY_TOKEN")
    role_arn = os.getenv("TEST_ATHENA_ROLE_ARN")

    if not web_identity_token or not role_arn:
        pytest.skip("Skipping OIDC test: web identity token or role ARN not set")

    return {
        "s3_staging_dir": os.getenv("TEST_ATHENA_OIDC_S3_STAGING_DIR"),
        "region_name": os.getenv("TEST_ATHENA_OIDC_REGION_NAME", "us-west-1"),
        "schema_name": "test",
        "role_arn": role_arn,
        "web_identity_token": web_identity_token,
    }


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    yield
    config.set_remote_function_list_path(None)
