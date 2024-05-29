#!/bin/bash
export ENV_MAX_HEAP_SIZE=$2
export ENV_INI_HEAP_SIZE=$3
python3 wren-sqlglot-server/main.py &

# Required add-opens=java.nio=ALL-UNNAMED for Apache arrow in the Snowflake
java -Xmx${ENV_MAX_HEAP_SIZE:-"512m"} -Xms${ENV_INI_HEAP_SIZE:-"64m"}  -Dconfig=etc/config.properties \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar $1
