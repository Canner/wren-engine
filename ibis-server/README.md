# Ibis server

## Environment Setup
- Python 3.11
- Install `poetry` with version 1.7.1: `curl -sSL https://install.python-poetry.org | python3 - --version 1.7.1`
- Execute `make install` to install the dependencies
- Execute `make test` to run the tests
- Create `.env` file and fill in the environment variables

## Environment Variables
- `WREN_ENGINE_ENDPOINT`: The endpoint of the Wren engine

## Start the server
- Execute `make run` to start the server
- Execute `make dev` to start the server in development mode. It will auto-reload after the code is edited.
- Default port is `8000`, you can change it by `make run PORT=8001` or `make dev PORT=8001`

## Docker
- Build the image: `make docker-build`
- Run the container: `make docker-run`