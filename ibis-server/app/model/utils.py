from duckdb import DuckDBPyConnection, HTTPException

from app.model import (
    GcsFileConnectionInfo,
    MinioFileConnectionInfo,
    S3FileConnectionInfo,
)


def init_duckdb_s3(
    connection: DuckDBPyConnection, connection_info: S3FileConnectionInfo
):
    create_secret = f"""
    CREATE SECRET wren_s3 (
        TYPE S3,
        KEY_ID '{connection_info.access_key.get_secret_value()}',
        SECRET '{connection_info.secret_key.get_secret_value()}',
        REGION '{connection_info.region.get_secret_value()}'
    )
    """
    try:
        result = connection.execute(create_secret).fetchone()
        if result is None or not result[0]:
            raise Exception("Failed to create secret")
    except HTTPException as e:
        raise Exception("Failed to create secret", e)


def init_duckdb_minio(
    connection: DuckDBPyConnection, connection_info: MinioFileConnectionInfo
):
    create_secret = f"""
    CREATE SECRET wren_minio (
        TYPE S3,
        KEY_ID '{connection_info.access_key.get_secret_value()}',
        SECRET '{connection_info.secret_key.get_secret_value()}',
        REGION 'ap-northeast-1'
    )
    """
    try:
        result = connection.execute(create_secret).fetchone()
        if result is None or not result[0]:
            raise Exception("Failed to create secret")
        connection.execute(
            "SET s3_endpoint=?", [connection_info.endpoint.get_secret_value()]
        )
        connection.execute("SET s3_url_style='path'")
        connection.execute("SET s3_use_ssl=?", [connection_info.ssl_enabled])
    except HTTPException as e:
        raise Exception("Failed to create secret", e)


def init_duckdb_gcs(
    connection: DuckDBPyConnection, connection_info: GcsFileConnectionInfo
):
    create_secret = f"""
    CREATE SECRET wren_gcs (
        TYPE GCS,
        KEY_ID '{connection_info.key_id.get_secret_value()}',
        SECRET '{connection_info.secret_key.get_secret_value()}'
    )
    """
    try:
        result = connection.execute(create_secret).fetchone()
        if result is None or not result[0]:
            raise Exception("Failed to create secret")
    except HTTPException as e:
        raise Exception("Failed to create secret", e)
