FROM python:3.11.9-slim as builder

# libpq-dev is required for psycopg2
RUN apt-get update && apt-get -y install libpq-dev gcc

    # python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    # poetry
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_CACHE_DIR=/tmp/poetry_cache

RUN pip install poetry==1.7.1

WORKDIR /app

COPY pyproject.toml ./

RUN poetry install --without dev && rm -rf $POETRY_CACHE_DIR

FROM python:3.11.9-slim as runtime

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

COPY app app

EXPOSE 8000

ENTRYPOINT fastapi run