# Description

It's an example docker-compose project to run Wren Engine.

# How to used

1. set up the platform in `.env` file. (`linux/amd64`, `linux/arm64`)
2. set up the configuration in `etc/config.properties` file.
3. put your MDL in `etc/mdl` file after removing the sample MDL file `etc/mdl/sample.json`.
    - The MDL folder should have only one json file.
4. set up the accounts if you needs or remove the sample accounts if you don't need.
    - There are some sample accounts in `etc/accounts`.
5. run the docker-compose
    ```bash
    docker compose --env-file .env up
    ```
6. Connect with psql or other pg driver with the port 7432.
    - The sample username and password are `ina` and `wah` or `azki` and `guess`.
    - The default database name should be the catalog of the MDL file.
    - The default schema name should be the schema of the MDL file.
   ```bash
    psql 'host=localhost user=ina dbname=test_catalog port=7432 options=--search_path=test_schema'
    ```
