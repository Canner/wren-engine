# Description

This is an example Docker Compose project for DuckDB data source with some tpch data.
You can learn how to set up the Wren Engine with DuckDB data source to analyze your data.

# How to use

1. Set up the platform in `.env` file. (`linux/amd64`, `linux/arm64`)
2. Configure settings in the `etc/config.properties` file.
3. Place your data in `etc/data` file after removing the sample data files.
4. Set up your initial SQL and session SQL in `etc/duckdb-init.sql` and `etc/duckdb-session.sql` files.
5. Place your MDL in `etc/mdl` file after removing the sample MDL file `etc/mdl/sample.json`.
    - The `mdl` directory should contain only one json file.
7. Run the docker-compose in this directory.
    ```bash
    docker compose --env-file .env up
    ```
8. Call the Wren Engine API to analyze your data with the request body as below.
   - URL
      ```
      GET http://localhost:8080/v1/mdl/preview
      ```
   - Body: The manifest is the content of the MDL file.
      ```json
         {
            "manifest": {
                           "catalog": "wren",
                           "schema": "tpch",
                           "models": [
                              {
                                 "name": "Orders",
                                 "tableReference": {
                                    "catalog": "memory",
                                    "schema": "tpch",
                                    "table": "orders"
                                 },
                                 "columns": [
                                    {
                                       "name": "orderkey",
                                       "expression": "o_orderkey",
                                       "type": "integer"
                                    },
                                    {
                                       "name": "custkey",
                                       "expression": "o_custkey",
                                       "type": "integer"
                                    },
                                    {
                                       "name": "orderstatus",
                                       "expression": "o_orderstatus",
                                       "type": "varchar"
                                    },
                                    {
                                       "name": "totalprice",
                                       "expression": "o_totalprice",
                                       "type": "float"
                                    }
                                 ],
                                 "primaryKey": "orderkey"
                              }
                           ]
                        },
            "sql": "select * from Orders" 
         }
      ```
