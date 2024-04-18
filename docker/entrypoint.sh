python3 wren-sqlglot-server/main.py &

# Required add-opens=java.nio=ALL-UNNAMED for Apache arrow in the Snowflake
java -Dconfig=etc/config.properties \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar $1
