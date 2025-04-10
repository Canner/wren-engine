# Description

This folder contains useful tools and scripts for debugging and validation.

# Tools
- `mdl_validation.py`: Used to validate a Wren MDL. This script attempts to select all columns in all models.
  - Requires the `wren_core` library. Run `just install-core` and `just install` before using it.
  - Example
    ```
    poetry run python tools/mdl_validation.py mdl.json function_list/bigquery.csv
    ```

- `query_local_run.py`: Execute a Wren SQL locally.
  - Requires the `wren_core` library. Run `just install-core` and `just install` before using it.
  - Some environment variables are required. See this script's documentation for details.
  - Example
    ```
    poetry run python tools/query_local_run.py < test.sql
    ```
  - Connection Info Example (BigQuery)
    ```json
    {
      "project_id": "wrenai",
      "dataset_id": "tpch",
      "credentials": "..."
    }
    ```
- `generate_openapi.py`: Used to generate the OpenAPI spec.
  - The generated yaml will follow the extension of [redoc](https://redocly.com/docs-legacy/api-reference-docs/spec-extensions).
  - It's helpful to create the API doc page by [redocusaurus](https://github.com/rohit-gohri/redocusaurus).
