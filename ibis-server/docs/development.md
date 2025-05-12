# Development

This document describes the process for running this application on your local computer.

## Getting started

This application is powered by Python and Rust! :sparkles: :snake: :gear: :sparkles:

It runs on macOS, Windows, and Linux environments.

You'll need `Python 3.11` to run the application. To install Python, [download the installer from python.org](https://www.python.org/downloads/).

You'll also need `Rust` and `Cargo`. To install them, follow the instructions on the [Rust installation page](https://www.rust-lang.org/tools/install).

Next, install the following tools:

- [poetry](https://github.com/python-poetry/poetry)
- [casey/just](https://github.com/casey/just)
- [pre-commit](https://pre-commit.com)
- [taplo](https://github.com/tamasfe/taplo)

After installing these tools, run:

```shell
just pre-commit-install
```

This installs the pre-commit hooks.

## Start the server

To get the application running:

1. Execute `just install` to install the dependencies
2. If you modified the core, you can install it into python venv by `just install-core`.
3. Create a `.env` file and fill in the required environment variables (see [Environment Variables](#Environment-Variables))

To start the server:

- Execute `just run` to start the server
- Execute `just dev` to start the server in development mode (auto-reloads on code changes)
- The default port is `8000`. You can change it by running `just port=8001 run` or `just port=8001 dev`

### Docker

- Build the image: `just docker-build`
- Run the container: `just docker-run`

### Run the testing

- Prepare the Java Engine server (see [Java Engine Example](../../example/README.md))
- Use the `.env` file to set the `WREN_ENGINE_ENDPOINT` environment variable to change the endpoint of the Java Engine server.
  ```
  WREN_ENGINE_ENDPOINT=http://localhost:8080
  ```
  More information about the environment variables can be found in the [Environment Variables](#Environment-Variables) section.
- Run specific data source test using [pytest marker](https://docs.pytest.org/en/stable/example/markers.html). There are some markers for different data sources. See the list
  in [pyproject.toml](../pyproject.toml).
  ```
  just test postgres
  ```

### Environment Variables

- `WREN_ENGINE_ENDPOINT`: The endpoint of the Wren Java engine
- `WREN_NUM_WORKERS`: The number of gunicoron workers

### OpenTelemetry Envrionment Variables
- `OTLP_ENABLED`: Enable the tracing for Ibis Server.
- See more [OpenTelemetry environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/).
- The minimize setting:
  ```
  OTLP_ENABLED=true
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://host.docker.internal:4317
  OTEL_SERVICE_NAME=wren-engine
  OTEL_TRACES_EXPORTER=otlp
  OTEL_METRICS_EXPORTER=none
  OTEL_LOGS_EXPORTER=none
  ```

## How to add new data source

Please see [How to Add a New Data Source](how-to-add-data-source.md) for more information.

## Troubleshooting

### No driver for MS SQL Server

If you want to run tests related to MS SQL Server, you may need to install the `Microsoft ODBC 18 driver`. \
You can follow the instructions to install the driver:

- [Microsoft ODBC driver for SQL Server (Linux)](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).
- [Microsoft ODBC driver for SQL Server (macOS)](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos).
- [Microsoft ODBC driver for SQL Server (Windows)](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### [ODBC Driver 18 for SQL Server]SSL Provider: [error:1416F086:SSL routines:tls_process_server_certificate:certificate verify failed:self signed certificate]

If you encounter this error, you can add the `TrustServerCertificate` parameter to the connection string.

```json
{
  "connectionInfo": {
    "kwargs": {
      "TrustServerCertificate": "YES"
    }
  }
}
```

### No driver for MySQL Server

If you want run tests related to MySQL Server or connect to MySQL through Wren Engine, you need to install the MySQL client libraries (e.g. `libmysqlclient`) by yourself.
- For linux, you can install `libmysqlclient-dev`. By the way, there are some different names for different linux versions. You should take care about it.
- For Mac, you can install `mysql-connector-c`
- For Windows, you can dowanload [the libraries](https://dev.mysql.com/downloads/c-api)


### Connect MySQL without SSL

By default, SSL mode is enabled and uses `caching_sha2_password` authentication, which only supports SSL connections. If you need to disable SSL, you must set SSLMode to DISABLED in your connection configuration to use `mysql_native_password` instead.

```json
{
  "connectionInfo": {
    "ssl_mode": "DISABLED"
  }
}
```
