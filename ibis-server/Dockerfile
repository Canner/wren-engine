FROM python:3.11-bookworm AS builder

ARG ENV
ENV ENV=$ENV


# libpq-dev is required for psycopg2
RUN apt-get update && apt-get -y install libpq-dev

# Install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"

# Install justfile
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/bin

    # python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # poetry
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1

RUN pip install poetry==1.8.3

COPY --from=wren-core-py . /wren-core-py
COPY --from=wren-core . /wren-core
COPY --from=wren-core-base . /wren-core-base

WORKDIR /app
COPY . .
RUN just install --without dev


FROM python:3.11-slim-bookworm AS runtime

# Add microsoft package list
RUN apt-get update \
    && apt-get install -y curl gnupg \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update

# Install msodbcsql 18 driver for mssql
RUN ACCEPT_EULA=Y apt-get -y install unixodbc-dev msodbcsql18

# Install libmysqlclient-dev for mysql
RUN apt-get install -y default-libmysqlclient-dev

# libpq-dev is required for psycopg2
RUN apt-get -y install libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app" \
    REMOTE_FUNCTION_LIST_PATH=/app/resources/function_list \
    REMOTE_WHITE_FUNCTION_LIST_PATH=/app/resources/white_function_list

# Set working directory before copying files
WORKDIR /app

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY app ./app/
COPY resources ./resources/
COPY notebooks ./notebooks/
COPY wren/ ./wren/

# Copy pyproject.toml and poetry.lock for poetry to work
COPY pyproject.toml poetry.lock ./

# Install poetry in runtime stage
RUN pip install poetry==1.8.3

# Install jupyter dependencies
RUN poetry install --without dev --with jupyter

# runtime
COPY --from=builder /app/.venv ${VIRTUAL_ENV}

# Install Opentelemetry zero-instrumentation python
RUN pip install opentelemetry-distro opentelemetry-exporter-otlp \
    && opentelemetry-bootstrap -a install

# Generate jupyter lab config
RUN jupyter lab --generate-config --allow-root

COPY entrypoint.sh ./
RUN chmod +x ./entrypoint.sh

EXPOSE 8000 8888

ENTRYPOINT ["./entrypoint.sh"]
