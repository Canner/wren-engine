# This script generates the OpenAPI specification for the application and saves it to a file.
# It's used to generate a yaml file that is used for API documentation builded by redoc.
# It will create a yaml file called `openai.yaml` at the current path.

from app.main import app
import yaml
import sys

# accept a input arguemnt for the version number
if len(sys.argv) != 2:
    print("Please provide the version number as an argument.")
    sys.exit(1)

version = sys.argv[1]

openapi_spec = app.openapi()
tags = []
if "components" in openapi_spec and "schemas" in openapi_spec["components"]:
    schemas = openapi_spec["components"]["schemas"]
    for schema_name, schema in schemas.items():
        if schema_name.endswith("ConnectionInfo") and "properties" in schema:
            for prop in schema["properties"].values():
                # To show the model in the doc page. Remove the writeOnly property.
                prop.pop("writeOnly", None)
            tags.append({
                "name": schema_name,
                "x-displayName": schema_name,
                "description": f"<SchemaDefinition schemaRef=\"#/components/schemas/{schema_name}\" />",
            })

# redoc extension: https://redocly.com/docs-legacy/api-reference-docs/spec-extensions
openapi_spec["tags"] = tags
openapi_spec["x-tagGroups"] = [
    {
        "name": "connector",
        "tags": ["connector"],
    },
    {
        "name": "analysis",
        "tags": ["analysis"],
    },
    {
        "name": "model",
        "tags": [ tag["name"] for tag in openapi_spec["tags"]],
    }
]
openapi_spec["info"]["version"] = version


with open("openapi.yaml", "w") as f:
    yaml.dump(openapi_spec, f, default_flow_style=False)
