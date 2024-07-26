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
2. Create a `.env` file and fill in the required environment variables (see [Environment Variables](#Environment-Variables))

To start the server:
- Execute `just run` to start the server
- Execute `just dev` to start the server in development mode (auto-reloads on code changes)
- The default port is `8000`. You can change it by running `just port=8001 run` or `just port=8001 dev`

### Docker
- Build the image: `just docker-build`
- Run the container: `just docker-run`

### Run the testing
- Prepare the Wren Engine server (see [Wren Engine Example](../../example/README.md))
- Use the `.env` file to set the `WREN_ENGINE_ENDPOINT` environment variable to change the endpoint of the Wren Engine server.
  ```
  WREN_ENGINE_ENDPOINT=http://localhost:8080
  ```
  More information about the environment variables can be found in the [Environment Variables](#Environment-Variables) section.
- Run specific data source test using [pytest marker](https://docs.pytest.org/en/stable/example/markers.html). There are some markers for different data sources. See the list in [pyproject.toml](../pyproject.toml).
  ```
  just test postgres
  ```

### Environment Variables
- `WREN_ENGINE_ENDPOINT`: The endpoint of the Wren Java engine


## How to add new data source
Please see [How to Add a New Data Source](how-to-add-data-source.md) for more information.


## Troubleshooting
### MS SQL Server Tests
If you're having trouble running tests related to MS SQL Server, you may need to install the appropriate driver. For Linux or macOS, we recommend installing the `unixodbc` and `freetds` packages.
After installation, run `odbcinst -j` to check the path of the `odbcinst.ini` file. Then, find the path of `libtdsodbc.so` in freetds and add the following content to the odbcinst.ini file:
```ini
[FreeTDS]
Description = FreeTDS driver
Driver = /opt/homebrew/Cellar/freetds/1.4.17/lib/libtdsodbc.so
Setup = /opt/homebrew/Cellar/freetds/1.4.17/lib/libtdsodbc.so
FileUsage = 1
```
Adjust the paths as necessary for your system.

