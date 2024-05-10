# Ibis server

## Environment Setup
- Python 3.11
- Install `poetry` with version 1.7.1: `curl -sSL https://install.python-poetry.org | python3 - --version 1.7.1`
- Execute `poetry install` to install the dependencies
- Execute `make test` to run the tests

## Start the server
- Execute `make start` to start the server
- Default port is `8000`, you can change it by setting the environment variable `IBIS_PORT`