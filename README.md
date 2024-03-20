# WrenEngine

# How to use?

Please refer to the [example](example/README.md) for further details. Start the engine using Docker Compose.

# How to build?

## Normal Build

```bash
mvn clean install -DskipTests
```

## Build an executable jar

```bash
mvn clean package -DskipTests -P exec-jar
```