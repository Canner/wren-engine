# Legacy Wren Core
It's the legacy version of the Wren core implemented in Java. This module is utilized by the API v2 of the ibis-server for SQL planning and is responsible for building the `wren-engine` Docker image.
Currently, Wren engine has been migrated to [a new Rust implementation](../wren-core/) 🚀.

## Requirements
- JDK 21+

## Running Wren Core Server
We recommend running Wren core server using Docker. It's the easiest way to start the wren core server:
```
docker run --name java-engine -p 8080:8080 -v $(pwd)/docker/etc:/usr/src/app/etc ghcr.io/canner/wren-engine:latest  
```

### Maven Build
For developing, you can build Wren core by yourself. Wren core is a standard maven project. We can build an executable jar using the following command:
```
./mvnw clean install -DskipTests -P exec-jar
```
Then, start Wren core server
```
java -Dconfig=docker/etc/config.properties --add-opens=java.base/java.nio=ALL-UNNAMED -jar wren-server/target/wren-server-0.15.2-SNAPSHOT-executable.jar
```

### Running Wren Engine in IDE
After building with Maven, you can run the project in your IDE. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Since Wren core is a standard Maven project, you can easily import it into your IDE. In IntelliJ, choose `Open Project from the Quick Start` box or select `Open` from the File menu and choose the root `pom.xml` file.

After opening the project in IntelliJ, ensure that the Java SDK is properly configured for the project:

1. Open the File menu and select **Project Structure**.
2. In the **SDKs** section, ensure that JDK 21 is selected (create one if it does not exist).
3. In the **Project** section, ensure the Project language level is set to 21.

Set up the running profile with the following configuration:
- **SDK**: The JDK you configured.
- **Main class**: `io.wren.server.WrenServer`
- **VM options**: `-Dconfig=docker/etc/config.properties`
- **Working directory**: The path to `wren-core-legacy`.