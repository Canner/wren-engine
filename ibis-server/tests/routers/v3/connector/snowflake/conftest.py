import os
import pathlib

import pytest
import snowflake.connector

pytestmark = pytest.mark.snowflake

base_url = "/v3/connector/snowflake"


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(autouse=True)
def init_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=os.getenv("SNOWFLAKE_PRIVATE_KEY"),
        warehouse="COMPUTE_WH",
    )
    cs = conn.cursor()
    try:
        cs.execute("USE WREN")
    except Exception:
        cs.execute("CREATE DATABASE IF NOT EXISTS WREN")
        cs.execute("USE WREN")
    try:
        cs.execute("USE SCHEMA PUBLIC")
    except Exception:
        cs.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
        cs.execute("USE SCHEMA PUBLIC")
    # prepare table with variant column
    cs.execute(
        """
        CREATE OR REPLACE TABLE car_sales
        ( 
        src variant
        )
        AS
        SELECT PARSE_JSON(column1) AS src
        FROM VALUES
        ('{ 
            "date" : "2017-04-28", 
            "dealership" : "Valley View Auto Sales",
            "salesperson" : {
            "id": "55",
            "name": "Frank Beasley"
            },
            "customer" : [
            {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
            ],
            "vehicle" : [
            {"make": "Honda", "model": "Civic", "year": "2017", "price": "20275", "extras":["ext warranty", "paint protection"]}
            ]
        }'),
        ('{ 
            "date" : "2017-04-28", 
            "dealership" : "Tindel Toyota",
            "salesperson" : {
            "id": "274",
            "name": "Greg Northrup"
            },
            "customer" : [
            {"name": "Bradley Greenbloom", "phone": "12127593751", "address": "New York, NY"}
            ],
            "vehicle" : [
            {"make": "Toyota", "model": "Camry", "year": "2017", "price": "23500", "extras":["ext warranty", "rust proofing", "fabric protection"]}  
            ]
        }') v;
        """
    )
    cs.close()
    conn.close()


@pytest.fixture(scope="module")
def tpch_connection_info() -> dict[str, str]:
    return {
        "user": os.getenv("SNOWFLAKE_USER"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "database": "SNOWFLAKE_SAMPLE_DATA",
        "schema": "TPCH_SF1",
        "warehouse": "COMPUTE_WH",
        "private_key": os.getenv("SNOWFLAKE_PRIVATE_KEY"),
    }


@pytest.fixture(scope="module")
def snowflake_connection_info() -> dict[str, str]:
    return {
        "user": os.getenv("SNOWFLAKE_USER"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "database": "WREN",
        "schema": "PUBLIC",
        "warehouse": "COMPUTE_WH",
        "private_key": os.getenv("SNOWFLAKE_PRIVATE_KEY"),
    }
