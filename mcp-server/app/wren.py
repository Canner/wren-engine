import base64
from dotenv import load_dotenv
import orjson
import json

import os
import httpx
from mcp.server.fastmcp import FastMCP
from dto import Manifest, TableColumns
from utils import dict_to_base64_string, json_to_base64_string

mcp = FastMCP("Wren Engine")

load_dotenv()
WREN_URL = os.getenv("WREN_URL", "localhost:8000")
USER_AGENT = "wren-app/1.0"
MDL_SCHEMA_PATH = "mdl.schema.json"
connection_info_path = os.getenv("CONNECTION_INFO_FILE")
# TODO: maybe we should log the number of tables and columns
mdl_path = os.getenv("MDL_PATH")

if mdl_path:
    with open(mdl_path) as f:
        mdl_schema = json.load(f)
        data_source = mdl_schema["dataSource"].lower()
        mdl_base64 = dict_to_base64_string(mdl_schema)
        print(f"Loaded MDL {f.name}")  # noqa: T201
else:
    print("No MDL_PATH environment variable found")

if connection_info_path:
    with open(connection_info_path) as f:
        connection_info = json.load(f)
        print(f"Loaded connection info {f.name}")  # noqa: T201
else:
    print("No CONNECTION_INFO_FILE environment variable found")


async def make_query_request(sql: str, dry_run: bool = False):
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"http://{WREN_URL}/v3/connector/{data_source}/query?dry_run={dry_run}",
                headers=headers,
                json={
                    "sql": sql,
                    "manifestStr": mdl_base64,
                    "connectionInfo": connection_info,
                },
                timeout=30,
            )
            return response
        except Exception as e:
            return e


async def make_table_list_request():
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"http://{WREN_URL}/v2/connector/{data_source}/metadata/tables",
                headers=headers,
                json={"connectionInfo": connection_info},
            )
            return response.text
        except Exception as e:
            return e


async def make_constraints_list_request():
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"http://{WREN_URL}/v2/connector/{data_source}/metadata/constraints",
                headers=headers,
                json={"connectionInfo": connection_info},
            )
            return response.text
        except Exception as e:
            return e


@mcp.resource("resource://mdl_json_schema")
async def get_mdl_json_schema() -> str:
    """
    Get the MDL JSON schema
    """
    with open(MDL_SCHEMA_PATH) as f:
        return f.read()


# TODO: should validate the MDL
@mcp.tool()
async def deploy(mdl: Manifest) -> str:
    global mdl_base64
    """
    Deploy the MDL JSON schema to Wren Engine
    """
    mdl_base64 = json_to_base64_string(mdl.model_dump_json(by_alias=True))
    return "MDL deployed successfully"


@mcp.tool()
async def is_deployed() -> str:
    """
    Check if the MDL JSON schema is deployed
    """
    if mdl_base64:
        return "MDL is deployed"
    return "MDL is not deployed. Please deploy the MDL first"


@mcp.tool()
async def list_remote_constraints() -> str:
    """
    Get the available constraints of connected Database
    """
    response = await make_constraints_list_request()
    return response


@mcp.tool()
async def list_remote_tables() -> str:
    """
    Get the available tables of connected Database
    """
    response = await make_table_list_request()
    return response


@mcp.tool()
async def query(sql: str) -> str:
    """
    Query the Wren Engine with the given SQL query
    """
    response = await make_query_request(sql)
    return response.text


@mcp.tool()
async def dry_run(sql: str) -> str:
    """
    Dry run the query in Wren Engine with the given SQL query.
    It's a cheap way to validate the query. It's better to have
    dry run before running the actual query.
    """
    try:
        await make_query_request(sql, True)
        return "Query is valid"
    except Exception as e:
        return e


@mcp.resource("wren://metadata/manifest")
async def get_full_manifest() -> str:
    """
    Get the current deployed manifest in Wren Engine
    """
    return base64.b64decode(mdl_base64).decode("utf-8")


@mcp.tool()
async def get_manifest() -> str:
    """
    Get the current deployed manifest in Wren Engine.
    If the number of deployed tables and columns is small, then it's better to use this tool.
    Otherwise, use `get_available_tables` and `get_table_info` and `get_column_info` tools.
    """
    return base64.b64decode(mdl_base64).decode("utf-8")


@mcp.resource("wren://metadata/tables")
async def get_available_tables_resource() -> str:
    """
    Get the available tables in Wren Engine
    """
    mdl = orjson.loads(base64.b64decode(mdl_base64).decode("utf-8"))
    # return only table name
    return [table["name"] for table in mdl["models"]]


@mcp.tool()
async def get_available_tables() -> str:
    """
    Get the available tables in Wren Engine
    """
    mdl = orjson.loads(base64.b64decode(mdl_base64).decode("utf-8"))
    # return only table name
    return [table["name"] for table in mdl["models"]]


@mcp.tool()
async def get_table_columns_info(
    table_columns: list[TableColumns], full_column_info: bool = False
) -> str:
    """
    Batch get the column info for the given table and column names in Wren Engine
    If the number of deployed tables and columns is huge, then it's better to use this tool to get only the required table and column info.

    Given a `TableColumns` object, if the columns isn't provided, then it will return all columns of the table.
    If the columns is not None, then it will return only the given columns of the table.
    If the `full_column_info` is True, then it will return the full column info, otherwise only the column name.
    """
    mdl = orjson.loads(base64.b64decode(mdl_base64).decode("utf-8"))
    result = []
    for table_column in table_columns:
        # find the specific table
        tables = [
            table for table in mdl["models"] if table["name"] == table_column.table_name
        ]

        if len(tables) == 0:
            return f"Table not found: {table_column.table_name}"

        if len(tables) > 1:
            return f"Multiple tables found: {table_column.table_name}"

        # extract the only column
        if table_column.column_names and len(table_column.column_names) > 0:
            columns = [
                col
                for col in tables[0]["columns"]
                if col["name"] in table_column.column_names
            ]
            # check the missed columns
            missed_columns = set(table_column.column_names) - set(
                [col["name"] for col in columns]
            )
            if len(missed_columns) > 0:
                return f"Table {table_column.table_name}'s columns not found: {missed_columns}"
        else:
            columns = tables[0]["columns"]

        if not full_column_info:
            columns = [col["name"] for col in columns]

        result.append({"table_name": table_column.table_name, "columns": columns})
    return orjson.dumps(result).decode("utf-8")


@mcp.tool()
async def get_table_info(table_name: str) -> str:
    """
    Get the table info for the given table name in Wren Engine
    """
    mdl = orjson.loads(base64.b64decode(mdl_base64).decode("utf-8"))
    result = [table for table in mdl["models"] if table["name"] == table_name]
    for table in result:
        table["columns"] = [col["name"] for col in table["columns"]]
    return orjson.dumps(result).decode("utf-8")


@mcp.tool()
async def get_column_info(table_name: str, column_name: str) -> str:
    """
    Get the column info for the given table and column name in Wren Engine
    """
    mdl = orjson.loads(base64.b64decode(mdl_base64).decode("utf-8"))
    # find the specific table
    tables = [table for table in mdl["models"] if table["name"] == table_name]

    if len(tables) == 0:
        return "Table not found"

    if len(tables) > 1:
        return "Multiple tables found"

    # extract the only column
    result = [col for col in tables[0]["columns"] if col["name"] == column_name]
    if len(result) == 0:
        return "Column not found"

    if len(result) > 1:
        return "Multiple columns found"

    return orjson.dumps(result[0]).decode("utf-8")


@mcp.tool()
async def get_relationships() -> str:
    """
    Get the relationships in Wren Engine
    """
    mdl = orjson.loads(base64.b64decode(mdl_base64).decode("utf-8"))
    return orjson.dumps(mdl["relationships"]).decode("utf-8")


# TODO: implement this tool
# @mcp.tool()
# async def get_available_functions() -> str:
#     pass


@mcp.tool()
async def health_check() -> str:
    """
    Check the health of Wren Engine
    """
    try:
        response = await make_query_request("SELECT 1")
        if response.status_code == 200:
            return "Wren Engine is healthy"
        else:
            return "Wren Engine is not healthy"
    except Exception as e:
        return "Wren Engine is not healthy"


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport="stdio")
