FROM python:3.11-buster as builder

# libpq-dev is required for psycopg2
RUN apt-get update && apt-get -y install libpq-dev

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

RUN pip install poetry==1.7.1

WORKDIR /app

COPY pyproject.toml ./
COPY poetry.lock ./
COPY rust/Cargo.toml ./
COPY rust/Cargo.lock ./

RUN poetry install --without dev
RUN poetry run maturin develop

FROM python:3.11-slim-buster as runtime

# libpq-dev is required for psycopg2
RUN apt-get update  \
    && apt-get -y install libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY python/wren_engine app

EXPOSE 8000

ENTRYPOINT fastapi run
