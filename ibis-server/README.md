# Ibis server
Ibis server is a server base on Python that provides the API for rewriting the queries with [Modeling Definition Language (MDL)](https://docs.getwren.ai/engine/concept/what_is_mdl) manifest and connect data source via [Ibis](https://github.com/ibis-project/ibis). It is built on top of the [FastAPI](https://github.com/tiangolo/fastapi) framework. \
We still need to run the Java engine for rewriting the queries. In the future, we will rewrite the modeling core of java engine to [Rust](https://github.com/rust-lang/rust) and integrate the modeling core into the Ibis server.

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
make install
```
Run the server
```bash
make run
```

## Developer Guide

### Environment Setup
- Python 3.11
- Install `poetry` with version 1.7.1: `curl -sSL https://install.python-poetry.org | python3 - --version 1.7.1`
- Install pre-commit hooks: `make pre-commit-install`
- Execute `make install` to install the dependencies
- Execute `make test` to run the tests
- Create `.env` file and fill in the environment variables

### Environment Variables
- `WREN_ENGINE_ENDPOINT`: The endpoint of the Wren Java engine
- `LOG_LEVEL`: The log level of the server. Default is `INFO`

### Start the server
- Execute `make run` to start the server
- Execute `make dev` to start the server in development mode. It will auto-reload after the code is edited.
- Default port is `8000`, you can change it by `make run PORT=8001` or `make dev PORT=8001`

### Docker
- Build the image: `make docker-build`
- Run the container: `make docker-run`
