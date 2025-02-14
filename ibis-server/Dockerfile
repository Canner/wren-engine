FROM python:3.11-buster AS builder

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


FROM python:3.11-slim-buster AS runtime

# Add microsoft package list
RUN apt-get update \
    && apt-get install -y curl gnupg \
    && curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
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
    REMOTE_FUNCTION_LIST_PATH=/resources/function_list

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY app app
COPY resources resources

EXPOSE 8000

CMD ["fastapi", "run"]
