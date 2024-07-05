# Ibis server
Ibis server is a server base on Python that provides the API for rewriting the queries with [Modeling Definition Language (MDL)](https://docs.getwren.ai/engine/concept/what_is_mdl) manifest and connect data source via [Ibis](https://github.com/ibis-project/ibis). It is built on top of the [FastAPI](https://github.com/tiangolo/fastapi) framework. \
We still need to run the Java engine for rewriting the queries. In the future, we will redesign the modeling core of java engine to [Rust](https://github.com/rust-lang/rust) and integrate the modeling core into the Ibis server. \
We continuously integrate the Rust core with the Ibis server now.

## Quick Start
### Running on Docker
You can follow the steps below to run the Java engine and ibis.
- Create `compose.yaml` file and add the following content, edit environment variables if needed (see [Environment Variables](#Environment-Variables))
```yaml
services:
  ibis:
    image: ghcr.io/canner/wren-engine-ibis:latest
    ports:
      - "8000:8000"
    environment:
      - WREN_ENGINE_ENDPOINT=http://java-engine:8080
      - LOG_LEVEL=INFO
  java-engine:
    image: ghcr.io/canner/wren-engine:latest
    expose:
      - "8080"
    volumes:
      - ./etc:/usr/src/app/etc
```
- Create `etc` directory and create `config.properties` inside the etc directory
```bash
mkdir etc
cd etc
vim config.properties
```
- Add the following content to the `config.properties` file
```bash
node.environment=production
wren.directory=/usr/src/app/etc/mdl
```
- Run the docker compose
```bash
docker compose up
```
### Running on Local
Requirements:
- Python 3.11
- [poetry](https://github.com/python-poetry/poetry) 1.7.1 (see [Environment Setup](#Environment-Setup))
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
Create `.env` file and fill in the environment variables (see [Environment Variables](#Environment-Variables))
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

## Developer Guide

### Environment Setup
- Python 3.11
- Install [Rust](https://www.rust-lang.org/tools/install) and [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- Install [poetry](https://github.com/python-poetry/poetry) with version 1.7.1: `curl -sSL https://install.python-poetry.org | python3 - --version 1.7.1`
- Install [casey/just](https://github.com/casey/just)
- Install [pre-commit](https://pre-commit.com) hooks: `just pre-commit-install`
- Install [taplo](https://github.com/tamasfe/taplo) for TOML linting

### Modeling Core
The modeling core is written in Rust and provides the Python adaptor. \
You must create python venv and install dependencies in [wren-modeling-py](../wren-modeling-py) first.

### Test and build
- Execute `just install` to install the dependencies
- Execute `just test` to run the tests
- Create `.env` file and fill in the environment variables

### Environment Variables
- `WREN_ENGINE_ENDPOINT`: The endpoint of the Wren Java engine
- `LOG_LEVEL`: The log level of the server. Default is `INFO`

### Start the server
- Execute `just run` to start the server
- Execute `just dev` to start the server in development mode. It will auto-reload after the code is edited.
- Default port is `8000`, you can change it by `just port=8001 run` or `just port=8001 dev`

### Docker
- Build the image: `just docker-build`
- Run the container: `just docker-run`

## FAQ
### Why can’t I run test about MS SQL Server?
Maybe you don't have the driver for MS SQL Server. If your OS is Linux or MacOS, We recommend you install the `unixodbc` and `freetds` package. \
After installing, you can execute `odbcinst -j` to check the path of the `odbcinst.ini` file.
```bash
odbcinst -j

unixODBC 2.3.12
DRIVERS............: /opt/homebrew/etc/odbcinst.ini
SYSTEM DATA SOURCES: /opt/homebrew/etc/odbc.ini
FILE DATA SOURCES..: /opt/homebrew/etc/ODBCDataSources
USER DATA SOURCES..: /Users/your_username/.odbc.ini
SQLULEN Size.......: 8
SQLLEN Size........: 8
SQLSETPOSIROW Size.: 8
```
Find the path of `libtdsodbc.so` in `freetds` and open the `odbcinst.ini` file to add the following content
```bash
[FreeTDS]
Description = FreeTDS driver
Driver = /opt/homebrew/Cellar/freetds/1.4.17/lib/libtdsodbc.so
Setup = /opt/homebrew/Cellar/freetds/1.4.17/lib/libtdsodbc.so
FileUsage = 1
```
