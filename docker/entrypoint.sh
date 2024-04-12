python3 wren-sqlglot-server/main.py &

java -Dconfig=etc/config.properties \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar $1
