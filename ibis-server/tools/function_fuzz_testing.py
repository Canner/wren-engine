from wren_core import SessionContext
import os
import json
from dotenv import load_dotenv

load_dotenv(override=True)
function_list_path = os.getenv("REMOTE_FUNCTION_LIST_PATH")
connection_info_path = os.getenv("CONNECTION_INFO_PATH")
data_source = os.getenv("DATA_SOURCE")

with open(connection_info_path) as file:
    connection_info = json.load(file)

ctx = SessionContext(None, function_list_path + f"/{data_source}.csv")
funzz_cases = ctx.generate_scalar_function_fuzz_case()

for case in funzz_cases:
    print("# Fuzz Case:\n", case)
