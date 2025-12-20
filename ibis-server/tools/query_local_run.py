#
# The script below is a standalone script that can be used to run a SQL query locally.
#
# Argements:
# - sql: stdin input a SQL query
#
# Environment variables:
# - WREN_MANIFEST_JSON_PATH: path to the manifest JSON file
# - REMOTE_FUNCTION_LIST_PATH: path to the function list file
# - CONNECTION_INFO_PATH: path to the connection info file
# - DATA_SOURCE: data source name
#

import base64
import json
import os
from app.custom_sqlglot.dialects.wren import Wren
from app.model import BigQueryDatasetConnectionInfo, MSSqlConnectionInfo, MySqlConnectionInfo, OracleConnectionInfo, PostgresConnectionInfo, SnowflakeConnectionInfo
from app.model.connector import BigQueryConnector
from app.util import to_json
import sqlglot
import sys
import pandas as pd

from dotenv import load_dotenv
from wren_core import SessionContext
from app.model.data_source import DataSourceExtension
import wren_core

if sys.stdin.isatty():
    print("please provide the SQL query via stdin, e.g. `python query_local_run.py < test.sql`", file=sys.stderr)
    sys.exit(1)

sql = sys.stdin.read()


load_dotenv(override=True)
manifest_json_path = os.getenv("WREN_MANIFEST_JSON_PATH")
function_list_path = os.getenv("REMOTE_FUNCTION_LIST_PATH")
connection_info_path = os.getenv("CONNECTION_INFO_PATH")
data_source = os.getenv("DATA_SOURCE")

# Welcome message
print("### Welcome to the Wren Core Query Runner ###")
print("#")
print("# Manifest JSON Path:", manifest_json_path)
print("# Function List Path:", function_list_path)
print("# Connection Info Path:", connection_info_path)
print("# Data Source:", data_source)
print("# SQL Query:\n", sql)
print("#")

# Read and encode the JSON data
with open(manifest_json_path) as file:
    mdl = json.load(file)
    # Convert to JSON string
    json_str = json.dumps(mdl)
    # Encode to base64
    encoded_str = base64.b64encode(json_str.encode("utf-8")).decode("utf-8")

with open(connection_info_path) as file:
    connection_info = json.load(file)

# Extract the requried tables from the SQL query
extractor = wren_core.ManifestExtractor(encoded_str)
tables = extractor.resolve_used_table_names(sql)
print("# Tables used in the SQL query:", tables)
# Extract the manifest for the required tables
manifest = extractor.extract_by(tables)
encoded_str = wren_core.to_json_base64(manifest)

print("### Starting the session context ###")
print("#")
# Set the session properties here. It should be lowercase.
properties =  {}
properties = frozenset(properties.items())
print("### Session Properties ###")
for key, value in properties:
    print(f"# {key}: {value}")
session_context = SessionContext(encoded_str, function_list_path + f"/{data_source}.csv", properties, data_source)
planned_sql = session_context.transform_sql(sql)
print("# Planned SQL:\n", planned_sql)

# Transpile the planned SQL
if data_source == "mssql":
    # For mssql, we need to use the "tsql" dialect for reading
    dialect_sql = sqlglot.transpile(planned_sql, read=Wren, write="tsql")[0]
else:
    dialect_sql = sqlglot.transpile(planned_sql, read=Wren, write=data_source)[0]
print("# Dialect SQL:\n", dialect_sql)
print("#")

if data_source == "bigquery":
    connection_info = BigQueryDatasetConnectionInfo.model_validate_json(json.dumps(connection_info))
    connection = BigQueryConnector(connection_info)
elif data_source == "mysql":
    connection_info = MySqlConnectionInfo.model_validate_json(json.dumps(connection_info))
    connection = DataSourceExtension.get_mysql_connection(connection_info)
elif data_source == "postgres":
    connection_info = PostgresConnectionInfo.model_validate_json(json.dumps(connection_info))
    connection = DataSourceExtension.get_postgres_connection(connection_info)
elif data_source == "oracle":
    connection_info = OracleConnectionInfo.model_validate_json(json.dumps(connection_info))
    connection = DataSourceExtension.get_oracle_connection(connection_info)
elif data_source == "mssql":
    connection_info = MSSqlConnectionInfo.model_validate_json(json.dumps(connection_info))
    connection = DataSourceExtension.get_mssql_connection(connection_info)
elif data_source == "snowflake":
    connection_info = SnowflakeConnectionInfo.model_validate_json(json.dumps(connection_info))
    connection = DataSourceExtension.get_snowflake_connection(connection_info)
else:
    raise Exception("Unsupported data source:", data_source)

df = connection.sql(dialect_sql).limit(10).to_pyarrow()
print("### Result ###")
print("")
print(df)
json_str = to_json(df, dict())
print("### Result JSON ###")
print("")
print(json_str)