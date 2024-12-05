#
# This script is used to validate the MDL file by transforming the SQL queries
# in the MDL file to the SQL queries. This script only validate if it can be transformed
# by Wren core. It does not validate the SQL can be executed by the data source.
#
# Argements:
# - manifest_json_path: Path to the JSON file
# - function_list_path: Path to the function list CSV file
#

import argparse
import base64
import json

from loguru import logger
from wren_core import SessionContext

# Set up argument parsing
parser = argparse.ArgumentParser(description="Validate the MDL file")
parser.add_argument("manifest_json_path", help="Path to the JSON file")
parser.add_argument("function_list_path", help="Path to the function list CSV file")

args = parser.parse_args()

# Read and encode the JSON data
with open(args.manifest_json_path) as file:
    mdl = json.load(file)
    # Convert to JSON string
    json_str = json.dumps(mdl)
    # Encode to base64
    encoded_str = base64.b64encode(json_str.encode("utf-8")).decode("utf-8")

session_context = SessionContext(encoded_str, args.function_list_path)
error_cases = []
for model in mdl["models"]:
    for column in model["columns"]:
        # ignore hidden columns
        if column.get("isHidden") or column.get("relationship") is not None:
            continue
        sql = f"select \"{column['name']}\" from \"{model['name']}\""
        try:
            planned_sql = session_context.transform_sql(sql)
        except Exception as e:
            error_cases.append((model, column))
            logger.info(f"Error transforming {model['name']} {column['name']}")
            logger.debug(e)

if len(error_cases) > 0:
    raise Exception(f"Error transforming {len(error_cases)} columns")
