from duckdb import DuckDBPyConnection, HTTPException

from app.model import S3FileConnectionInfo


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
