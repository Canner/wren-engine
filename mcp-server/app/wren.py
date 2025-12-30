import base64
from dotenv import load_dotenv
import orjson
import json

import os
import httpx
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from dto import Manifest, TableColumns
from utils import dict_to_base64_string, json_to_base64_string

mcp = FastMCP("Wren Engine")

load_dotenv()
WREN_URL = os.getenv("WREN_URL", "localhost:8000")
USER_AGENT = "wren-app/1.0"
MDL_SCHEMA_PATH = "mdl.schema.json"
connection_info_path = os.getenv("CONNECTION_INFO_FILE")
mdl_path = os.getenv("MDL_PATH")


class MdlCache:
    """
    Cache for MDL with pre-built indexes for O(1) lookups.
    Avoids repeated base64 decoding and O(n) linear searches.
    """

    def __init__(self):
        self._mdl_base64: str | None = None
        self._mdl_dict: dict | None = None
        self._model_index: dict[str, dict] = {}
        self._column_index: dict[str, dict[str, dict]] = {}
        self._table_names: list[str] = []

    def set_mdl(self, mdl_base64: str) -> None:
        """Set MDL and invalidate cached indexes."""
        self._mdl_base64 = mdl_base64
        self._mdl_dict = None
        self._model_index = {}
        self._column_index = {}
        self._table_names = []

    def get_base64(self) -> str | None:
        return self._mdl_base64

    def is_deployed(self) -> bool:
        return self._mdl_base64 is not None

    def _ensure_parsed(self) -> None:
        """Lazy parse and build indexes on first access."""
        if self._mdl_dict is not None:
            return

        if self._mdl_base64 is None:
            self._mdl_dict = {}
            return

        self._mdl_dict = orjson.loads(
            base64.b64decode(self._mdl_base64).decode("utf-8")
        )

        models = self._mdl_dict.get("models", [])
        self._model_index = {model["name"]: model for model in models}
        self._table_names = list(self._model_index.keys())

        for model_name, model in self._model_index.items():
            columns = model.get("columns", [])
            self._column_index[model_name] = {col["name"]: col for col in columns}

    def get_mdl_dict(self) -> dict:
        """Get parsed MDL dictionary."""
        self._ensure_parsed()
        return self._mdl_dict

    def get_table_names(self) -> list[str]:
        """Get list of table names. O(1) after first call."""
        self._ensure_parsed()
        return self._table_names

    def get_model(self, table_name: str) -> dict | None:
        """Get model by name. O(1) lookup."""
        self._ensure_parsed()
        return self._model_index.get(table_name)

    def get_column(self, table_name: str, column_name: str) -> dict | None:
        """Get column by table and column name. O(1) lookup."""
        self._ensure_parsed()
        table_columns = self._column_index.get(table_name)
        if table_columns is None:
            return None
        return table_columns.get(column_name)

    def get_columns_dict(self, table_name: str) -> dict[str, dict] | None:
        """Get all columns for a table as dict. O(1) lookup."""
        self._ensure_parsed()
        return self._column_index.get(table_name)

    def get_relationships(self) -> list:
        """Get relationships from MDL."""
        self._ensure_parsed()
        return self._mdl_dict.get("relationships", [])


mdl_cache = MdlCache()
data_source = None

if mdl_path:
    with open(mdl_path) as f:
        mdl_schema = json.load(f)
        data_source = mdl_schema["dataSource"].lower()
        mdl_cache.set_mdl(dict_to_base64_string(mdl_schema))
        models = mdl_schema.get("models", [])
        total_columns = sum(len(m.get("columns", [])) for m in models)
        print(f"Loaded MDL {f.name} ({len(models)} models, {total_columns} columns)")  # noqa: T201
else:
    print("No MDL_PATH environment variable found")

if connection_info_path:
    with open(connection_info_path) as f:
        connection_info = json.load(f)
        print(f"Loaded connection info {f.name}")  # noqa: T201
else:
    print("No CONNECTION_INFO_FILE environment variable found")


async def make_query_request(sql: str, dry_run: bool = False):
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json", "x-wren-fallback_disable": "true"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"http://{WREN_URL}/v3/connector/{data_source}/query?dryRun={dry_run}",
                headers=headers,
                json={
                    "sql": sql,
                    "manifestStr": mdl_cache.get_base64(),
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

async def make_get_available_functions_request():
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"http://{WREN_URL}/v3/connector/{data_source}/functions",
                headers=headers,
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
@mcp.tool(
    annotations=ToolAnnotations(
        title="Deploy MDL",
        destructiveHint=True,
    ),
)
async def deploy(mdl: Manifest) -> str:
    """
    Deploy the MDL JSON schema to Wren Engine
    """
    global data_source
    mdl_json = mdl.model_dump_json(by_alias=True)
    mdl_dict = orjson.loads(mdl_json)
    data_source = mdl_dict.get("dataSource", "").lower()
    mdl_base64 = json_to_base64_string(mdl_json)
    mdl_cache.set_mdl(mdl_base64)
    return "MDL deployed successfully"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Check Deployment Status",
        readOnlyHint=True,
    ),
)
async def is_deployed() -> str:
    """
    Check if the MDL JSON schema is deployed
    """
    if mdl_cache.is_deployed():
        return "MDL is deployed"
    return "MDL is not deployed. Please deploy the MDL first"


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Remote Constraints",
        readOnlyHint=True,
    ),
)
async def list_remote_constraints() -> str:
    """
    Get the available constraints of connected Database
    """
    response = await make_constraints_list_request()
    return response


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Remote Tables",
        readOnlyHint=True,
    ),
)
async def list_remote_tables() -> str:
    """
    Get the available tables of connected Database
    """
    response = await make_table_list_request()
    return response


@mcp.tool(
    annotations=ToolAnnotations(
        title="Execute Query",
        destructiveHint=True,
    ),
)
async def query(sql: str) -> str:
    """
    Query the Wren Engine with the given SQL query
    """
    response = await make_query_request(sql)
    return response.text


@mcp.tool(
    annotations=ToolAnnotations(
        title="Dry Run Query",
        readOnlyHint=True,
    ),
)
async def dry_run(sql: str) -> str:
    """
    Dry run the query in Wren Engine with the given SQL query.
    It's a cheap way to validate the query. It's better to have
    dry run before running the actual query.
    """
    try:
        response = await make_query_request(sql, True)
        return response.text
    except Exception as e:
        return e


@mcp.resource("wren://metadata/manifest")
async def get_full_manifest() -> str:
    """
    Get the current deployed manifest in Wren Engine
    """
    return base64.b64decode(mdl_cache.get_base64()).decode("utf-8")


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Manifest",
        readOnlyHint=True,
    ),
)
async def get_manifest() -> str:
    """
    Get the current deployed manifest in Wren Engine.
    If the number of deployed tables and columns is small, then it's better to use this tool.
    Otherwise, use `get_available_tables` and `get_table_info` and `get_column_info` tools.
    """
    return base64.b64decode(mdl_cache.get_base64()).decode("utf-8")


@mcp.resource("wren://metadata/tables")
async def get_available_tables_resource() -> str:
    """
    Get the available tables in Wren Engine
    """
    return mdl_cache.get_table_names()


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Available Tables",
        readOnlyHint=True,
    ),
)
async def get_available_tables() -> list[str]:
    """
    Get the available tables in Wren Engine
    """
    return mdl_cache.get_table_names()   


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Table Columns Info",
        readOnlyHint=True,
    ),
)
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
    result = []
    for table_column in table_columns:
        model = mdl_cache.get_model(table_column.table_name)
        if model is None:
            return f"Table not found: {table_column.table_name}"

        columns_dict = mdl_cache.get_columns_dict(table_column.table_name)

        if table_column.column_names and len(table_column.column_names) > 0:
            columns = []
            missed_columns = []
            for col_name in table_column.column_names:
                col = columns_dict.get(col_name)
                if col is not None:
                    columns.append(col)
                else:
                    missed_columns.append(col_name)

            if len(missed_columns) > 0:
                return f"Table {table_column.table_name}'s columns not found: {set(missed_columns)}"
        else:
            columns = list(columns_dict.values())

        if not full_column_info:
            columns = [col["name"] for col in columns]

        result.append({"table_name": table_column.table_name, "columns": columns})
    return orjson.dumps(result).decode("utf-8")


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Table Info",
        readOnlyHint=True,
    ),
)
async def get_table_info(table_name: str) -> str:
    """
    Get the table info for the given table name in Wren Engine
    """
    model = mdl_cache.get_model(table_name)
    if model is None:
        return orjson.dumps([]).decode("utf-8")

    result = model.copy()
    result["columns"] = [col["name"] for col in model.get("columns", [])]
    return orjson.dumps([result]).decode("utf-8")


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Column Info",
        readOnlyHint=True,
    ),
)
async def get_column_info(table_name: str, column_name: str) -> str:
    """
    Get the column info for the given table and column name in Wren Engine
    """
    model = mdl_cache.get_model(table_name)
    if model is None:
        return "Table not found"

    column = mdl_cache.get_column(table_name, column_name)
    if column is None:
        return "Column not found"

    return orjson.dumps(column).decode("utf-8")


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Relationships",
        readOnlyHint=True,
    ),
)
async def get_relationships() -> str:
    """
    Get the relationships in Wren Engine
    """
    return orjson.dumps(mdl_cache.get_relationships()).decode("utf-8")


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Available Functions",
        readOnlyHint=True,
    ),
)
async def get_available_functions() -> str:
    """
    Get the available functions of connected DataSource Type
    """
    response = await make_get_available_functions_request()
    return response

@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Data Source Type",
        readOnlyHint=True,
    ),
)
async def get_current_data_source_type() -> str:
    """
    Get the current data source type
    """
    if data_source is None:
        return "No data source connected. Please deploy the MDL first and assign `dataSource` field."
    return data_source

@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Wren Guide",
        readOnlyHint=True,
    ),
)
async def get_wren_guide() -> str:
    """
    Understand how to use Wren Engine effectively to query your database
    """

    tips = f"""
    ## Tips for using Wren Engine with {data_source.capitalize()}
    You are connected to a {data_source.capitalize()} database via Wren Engine.
    Here are some tips to use {data_source.capitalize()} effectively:
    """

    if data_source == "snowflake":
        tips += """
        1. Snowflake supports semi-structured data types like VARIANT, OBJECT, and ARRAY. You can use these data types to store and query JSON data.
        2. Snowflake has a rich set of built-in functions to process semi-structured data. You can use functions like GET_PATH, TO_VARIANT, TO_ARRAY, etc.
        3. For process semi-structure data type (e.g. `VARIANT`), you can use `get_path` function to extract the value from the semi-structure data.
        4. For process array data type (e.g. `ARRAY`), you can use `UNNEST` function to flatten the array data. `UNNEST` only accepts array column as input. If you extract an array value by `get_path` function, you need to cast it to array type (by `to_array` function) before using `UNNEST`.
        """
    else:
        tips += f"""
        1. Use {data_source.capitalize()}'s specific functions and features to optimize your queries.
        2. Refer to {data_source.capitalize()}'s documentation for more details on how to use its features effectively.
        """

    return f"""
    Wren Engine is a semantic layer to help you query your database easily using SQL. It supports ANSI SQL to query your database.
    Wren SQL doesn't equal to your database SQL. Wren Engine translates your Wren SQL to your database SQL internally.
    Avoid to use database specific SQL syntax in your Wren SQL.
    The models you can access are defined in the MDL (Model Definition Language) schema you deployed.

    Here are some tips to use Wren Engine effectively:
    1. Use simple SQL queries to get data from your tables. You can use SELECT, WHERE, JOIN, GROUP BY, ORDER BY, etc.
    2. Use table and column names as defined in the MDL schema.
    3. Use aliases to make your queries more readable.
    4. Use functions supported by your data source type. You can get the list of available functions using the `get_available_functions` tool.
    5. Avoid to use `LATERAL` statement in your queries, as Wren Engine may not support it well. Use normal `JOIN` or `CROSS JOIN UNNEST` instead.
    6. Check the tips below for some general tips and some tips specific to the connected DataSource Type.
    {tips}

    ## General Tips
    1. If you encounter any issues, please check the health of Wren Engine using the `health_check` tool.
    2. You can also get the deployed MDL schema using the `get_manifest` tool.
    3. If the number of deployed tables and columns is huge, you can use `get_available_tables`, `get_table_info`, and `get_column_info` tools to get only the required table and column info.
    4. You can validate your SQL query using the `dry_run` tool before running the actual query.
    """

@mcp.tool(
    annotations=ToolAnnotations(
        title="Health Check",
        readOnlyHint=True,
    ),
)
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
