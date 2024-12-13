# Ibis server
Ibis server is a server base on Python that provides the API for rewriting the queries with [Modeling Definition Language (MDL)](https://docs.getwren.ai/engine/concept/what_is_mdl) manifest and connect data source via [Ibis](https://github.com/ibis-project/ibis). It is built on top of the [FastAPI](https://github.com/tiangolo/fastapi) framework. \
We still need to run the Java engine for rewriting the queries. In the future, we will redesign the modeling core of java engine to [Rust](https://github.com/rust-lang/rust) and integrate the modeling core into the Ibis server. \
We continuously integrate the Rust core with the Ibis server now.

## Application structure
The application consists of three main parts:
1. [ibis-server](./): a Python web server powered by FastAPI and Ibis
2. [wren-core](../wren-cores): a modeling core written in Rust powered by [Apache DataFusion](https://github.com/apache/datafusion)
3. [wren-core-py](../wren-core-py): a Python adapter for the modeling core

## Quick Start

### Running on Docker
You can follow the steps below to run the Java engine and ibis.
Create `compose.yaml` file and add the following content, edit environment variables if needed (see [Environment Variables](docs/development#environment-variables))
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
wren.directory=/usr/src/app/etc/mdl
```
Run the docker compose
```bash
docker compose up
```

### Running on Local
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

## Contributing
Please see [CONTRIBUTING.md](docs/CONTRIBUTING.md) for more information.
