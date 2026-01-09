import pathlib

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.spark

base_url = "/v3/connector/spark"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


function_list_path = file_path("../resources/function_list")


@pytest.fixture(scope="session")
def spark_connect(request):
    spark = (
        SparkSession.builder.remote("sc://localhost:15002")
        .appName("pytest-spark")
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS default")

    # Ensure clean state (IMPORTANT)
    spark.sql("DROP TABLE IF EXISTS default.orders")

    # Load test data
    orders_pdf = pd.read_parquet(file_path("resource/tpch/data/orders.parquet"))

    if hasattr(orders_pdf, "attrs"):
        orders_pdf.attrs = {}

    # Create Spark DataFrames and save as tables
    orders_df = spark.createDataFrame(orders_pdf)

    # Create managed table ONCE
    orders_df.write.mode("overwrite").saveAsTable("default.orders")

    def cleanup():
        # Drop test tables
        try:
            spark.sql("DROP TABLE IF EXISTS default.orders")
        except Exception as e:
            raise e
        finally:
            spark.stop()

    request.addfinalizer(cleanup)
    return spark


@pytest.fixture(scope="module")
def connection_info(spark_connect: SparkSession) -> dict[str, str]:
    return {
        "host": "localhost",
        "port": "15002",
    }


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    yield
    config.set_remote_function_list_path(None)
