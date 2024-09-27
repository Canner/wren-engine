FROM python:3.11-buster AS builder

# libpq-dev is required for psycopg2
RUN apt-get update && apt-get -y install libpq-dev

# TODO: enable rust after fix the issue with the build
# Install rust
# RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
# ENV PATH="/root/.cargo/bin:$PATH"

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

# TODO: enable rust after fix the issue with the build
# COPY --from=wren-modeling-py . /wren-modeling-py
# COPY --from=wren-modeling-rs . /wren-modeling-rs

WORKDIR /app
COPY . .
# TODO: enable rust after fix the issue with the build
# RUN just install --without dev
RUN poetry install --without dev


FROM python:3.11-slim-buster AS runtime

# libpq-dev is required for psycopg2
# unixodbc-dev, freetds-dev and tdsodbc are required for pyodbc
RUN apt-get update  \
    && apt-get -y install libpq-dev \
    && apt-get -y install unixodbc-dev freetds-dev tdsodbc \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETPLATFORM

# Install FreeTDS driver
RUN <<EOT
if [ "$TARGETPLATFORM" = "linux/amd64" ]; then
  export FREE_TDS_PATH=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so;
elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then
  export FREE_TDS_PATH=/usr/lib/aarch64-linux-gnu/odbc/libtdsodbc.so;
else \
  echo "Unsupported platform: $TARGETPLATFORM"; exit 1;
fi && \
echo "FREE_TDS_PATH is set to $FREE_TDS_PATH"
cat <<EOF > free.tds.ini
[FreeTDS]
Description = FreeTDS driver
Driver = ${FREE_TDS_PATH}
Setup = ${FREE_TDS_PATH}
EOF
odbcinst -i -d -f free.tds.ini
EOT

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY app app

EXPOSE 8000

CMD ["fastapi", "run"]
