# Ibis Server Module
This module is the API server of Wren Engine. It's built on top of [FastAPI](https://fastapi.tiangolo.com/). It provides several APIs for SQL queries. A SQL query will be planned by [wren-core](../wren-core/), transpiled by [sqlglot](https://github.com/tobymao/sqlglot), and then executed by [ibis](https://github.com/ibis-project/ibis) to query the database.

## Quick Start

### Running Ibis Server on Docker
You can follow the steps below to run the Java engine and ibis.
> Wren Engine is migrating to [wren-core](../wren-core/). However, we still recommend starting [the Java engine](../wren-core-legacy/) to enable the query fallback mechanism.

Create `compose.yaml` file and add the following content, edit environment variables if needed (see [Environment Variables](docs/development#environment-variables)).
```yaml
services:
  ibis:
    image: ghcr.io/canner/wren-engine-ibis:latest
    ports:
      - "8000:8000"
    environment:
      - WREN_ENGINE_ENDPOINT=http://java-engine:8080
  java-engine:
    image: ghcr.io/canner/wren-engine:latest
    expose:
      - "8080"
    volumes:
      - ./etc:/usr/src/app/etc
```
Create `etc` directory and create `config.properties` inside the etc directory
```bash
mkdir etc
cd etc
vim config.properties
```
Add the following content to the `config.properties` file
```bash
node.environment=production
```
Run the docker compose
```bash
docker compose up
```

Set up [OpenTelemetry Envrionment Variables](docs/development#environment-variable) to enable tracing log.
See [Tracing with Jaeger](#tracing-with-jaeger) to start up a Jaeger Server.

### Running Ibis Server on Local
Requirements:
- Python 3.11
- [casey/just](https://github.com/casey/just)
- [poetry](https://github.com/python-poetry/poetry)
- [Rust](https://www.rust-lang.org/tools/install) and [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)

Clone the repository
```bash
git clone git@github.com:Canner/wren-engine.git
```
Start Java engine for the feature rewriting-queries
```bash
cd example
docker compose --env-file .env up
```
Navigate to the `ibis-server` directory
```bash
cd ibis-server
```
Create `.env` file and fill in the environment variables (see [Environment Variables](docs/development#environment-variables))
```bash
vim .env
```
Install the dependencies
```bash
just install
```
Run the server
```bash
just run
```


### Running Spark Tests Locally
Spark-related tests require a local Spark cluster with Spark Connect enabled.
Our GitHub CI already handles this automatically, but you must start Spark manually when running tests locally.

Prerequisites

- Docker & Docker Compose
- Python dependencies installed (just install)
- Java engine running (same as other integration tests)

1. Start the Spark Cluster
From the ibis-server directory:
```
cd tests/routers/v3/connector/spark
docker compose up -d --build
```
Wait until Spark Connect is ready. You should see this in the logs:
```
docker logs spark-connect
# Spark Connect server started
```
- Spark Master: spark://localhost:7077
- Spark Connect: localhost:15002

2. Run Spark Tests
Go back to the ibis-server directory and run:
```
just test spark
```
⚠️ Spark tests will fail if the Spark cluster is not running.

3. Stop the Spark Cluster (Cleanup)
After tests finish:
```
cd tests/routers/v3/connector/spark
docker compose down -v
```



### Start with Python Interactive Mode
Install the dependencies
```bash
just install
```
Launch a CLI with an active Wren session using the following command:
```
python -m wren local_file <mdl_path> <connection_info_path>
```
This will create an interactive CLI environment with a `wren.session.Context` instance for querying your database.
```
Session created: Context(id=1352f5de-a8a7-4342-b2cf-015dbb2bba4f, data_source=local_file)
You can now interact with the Wren session using the 'wren' variable:
> task = wren.sql('SELECT * FROM your_table').execute()
> print(task.results)
> print(task.formatted_result())
Python 3.11.11 (main, Dec  3 2024, 17:20:40) [Clang 16.0.0 (clang-1600.0.26.4)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
(InteractiveConsole)
>>> 
```

### Start with Jupyter Notebook
Launch a Jupyter notebook server with Wren engine dependencies using Docker:
```
docker run --rm -p 8888:8888 ghcr.io/canner/wren-engine-ibis:latest jupyter
```
Explore the demo notebooks to learn how to use the Wren session context:
```
http://localhost:8888/lab/doc/tree/notebooks/demo.ipynb
```


### Enable Tracing
We use OpenTelemetry as its tracing framework. Refer to OpenTelemetry zero-code instrumentation to install the required dependencies.
Then, use the following just command to start the Ibis server, which exports tracing logs to the console:
```
just run-trace-console
```
OpenTelemetry zero-code instrumentation is highly configurable. You can set the necessary exporters to send traces to your tracing services.

[Metrics we are tracing right now](./Metrics.md)

### Tracing with Jaeger
- Follow the [Jaeger official documentation](https://www.jaegertracing.io/docs/2.5/getting-started/#all-in-one) to start Jaeger in a container. Use the following command:
```
docker run --rm --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 5778:5778 \
  -p 9411:9411 \
  jaegertracing/jaeger:2.5.0
```
- Install [OpenTelemetry Python zero-code instrumentation](https://opentelemetry.io/docs/zero-code/python/#setup)
```
pip install opentelemetry-distro opentelemetry-exporter-otlp
opentelemetry-bootstrap -a install
```
- Use the following `just` command to start the `ibis-server` and export tracing logs to Jaeger:
```
just run-trace-otlp 
```
- Open the Jaeger UI at [http://localhost:16686](http://localhost:16686) to view the tracing logs for your requests.

## Contributing
Please see [CONTRIBUTING.md](docs/CONTRIBUTING.md) for more information.

### Report the Migration Issue
Wren engine is migrating to v3 API (powered by Rust and DataFusion). However, there are some SQL issues currently.
If you find the migration message in your log, we hope you can provide the message and related information to the Wren AI Team.
Just raise an issue on GitHub or contact us in the Discord channel.

The message would look like the following log:
```
2025-03-19 22:49:08.788 | [62781772-7120-4482-b7ca-4be65c8fda96] | INFO     | __init__.dispatch:14 - POST /v3/connector/postgres/query
2025-03-19 22:49:08.788 | [62781772-7120-4482-b7ca-4be65c8fda96] | INFO     | __init__.dispatch:15 - Request params: {}
2025-03-19 22:49:08.789 | [62781772-7120-4482-b7ca-4be65c8fda96] | INFO     | __init__.dispatch:22 - Request body: {"connectionInfo":"REDACTED","manifestStr":"eyJjYXRhbG9nIjoid3JlbiIsInNjaGVtYSI6InB1YmxpYyIsIm1vZGVscyI6W3sibmFtZSI6Im9yZGVycyIsInRhYmxlUmVmZXJlbmNlIjp7InNjaGVtYSI6InB1YmxpYyIsIm5hbWUiOiJvcmRlcnMifSwiY29sdW1ucyI6W3sibmFtZSI6Im9yZGVya2V5IiwidHlwZSI6InZhcmNoYXIiLCJleHByZXNzaW9uIjoiY2FzdChvX29yZGVya2V5IGFzIHZhcmNoYXIpIn1dfV19","sql":"SELECT orderkey FROM orders LIMIT 1"}
2025-03-19 22:49:08.804 | [62781772-7120-4482-b7ca-4be65c8fda96] | WARN    | connector.query:61 - Failed to execute v3 query, fallback to v2: DataFusion error: ModelAnalyzeRule
caused by
Schema error: No field named o_orderkey.
Wren engine is migrating to Rust version now. Wren AI team are appreciate if you can provide the error messages and related logs for us.
```

#### Steps to Report an Issue
1. **Identify the Issue**: Look for the migration message in your log files.
2. **Gather Information**: Collect the error message and any related logs.
3. **Report the Issue**:
   - **GitHub**: Open an issue on our [GitHub repository](https://github.com/Canner/wren-engine/issues) and include the collected information.
   - **Discord**: Join our [Discord channel](https://discord.gg/5DvshJqG8Z) and share the details with us.

Providing detailed information helps us to diagnose and fix the issues more efficiently. Thank you for your cooperation!
