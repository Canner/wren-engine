# Ibis server

## Quick Start
### Running on Docker
Pull the image from the GitHub Container Registry
```bash
docker pull ghcr.io/canner/wren-engine-ibis:latest
```
Create `.env` file and fill in the environment variables (see [Environment Variables](#Environment-Variables)) \
```bash
vim .env
```
Run the container
```bash
docker run --env-file .env -p 8000:8000 ghcr.io/canner/wren-engine-ibis:latest
```
### Running on Local
Requirements:
- Python 3.11
- [poetry](https://github.com/python-poetry/poetry) 1.7.1

Clone the repository and navigate to the ibis directory
```bash
git clone git@github.com:Canner/wren-engine.git
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

## Contributing

### Environment Setup
- Python 3.11
- Install `poetry` with version 1.7.1: `curl -sSL https://install.python-poetry.org | python3 - --version 1.7.1`
- Execute `make install` to install the dependencies
- Execute `make test` to run the tests
- Create `.env` file and fill in the environment variables

### Environment Variables
- `WREN_ENGINE_ENDPOINT`: The endpoint of the Wren engine

### Start the server
- Execute `make run` to start the server
- Execute `make dev` to start the server in development mode. It will auto-reload after the code is edited.
- Default port is `8000`, you can change it by `make run PORT=8001` or `make dev PORT=8001`

### Docker
- Build the image: `make docker-build`
- Run the container: `make docker-run`