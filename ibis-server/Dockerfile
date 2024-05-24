FROM python:3.11.9-slim as builder

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
    POETRY_VIRTUALENVS_IN_PROJECT=true

RUN pip install poetry==1.7.1

WORKDIR /app

COPY pyproject.toml ./
COPY poetry.lock ./
COPY .venv ./

RUN poetry install --with ibis

FROM python:3.11.9-slim as runtime

# libpq-dev is required for psycopg2
RUN apt-get update  \
    && apt-get -y install libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv
COPY app app

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

ENTRYPOINT fastapi run
