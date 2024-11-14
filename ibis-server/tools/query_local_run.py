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
import sqlglot
import sys

from dotenv import load_dotenv
from wren_core import SessionContext
from app.model.data_source import BigQueryConnectionInfo
from app.model.data_source import DataSourceExtension

if sys.stdin.isatty():
    print("please provide the SQL query via stdin, e.g. `python query_local_run.py < test.sql`", file=sys.stderr)
    sys.exit(1)

sql = sys.stdin.read()


load_dotenv()
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

print("### Starting the session context ###")
print("#")
session_context = SessionContext(encoded_str, function_list_path)
planned_sql = session_context.transform_sql(sql)
print("# Planned SQL:\n", planned_sql)

# Transpile the planned SQL
dialect_sql = sqlglot.transpile(planned_sql, read="trino", write=data_source)[0]
print("# Dialect SQL:\n", dialect_sql)
print("#")

if data_source == "bigquery":
    connection_info = BigQueryConnectionInfo.model_validate_json(json.dumps(connection_info))
    connection = DataSourceExtension.get_bigquery_connection(connection_info)
    df = connection.sql(dialect_sql).limit(10).to_pandas()
    print("### Result ###")
    print("")
    print(df)
else:
    print("Unsupported data source:", data_source)