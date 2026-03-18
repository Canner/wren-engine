# WHEEL_SOURCE=docker (default): build Rust inside Docker with BuildKit cargo cache
# WHEEL_SOURCE=local: use pre-built Linux wheel from host (maturin + zig required)
ARG WHEEL_SOURCE=docker

# ── Option A: Build Rust wheel inside Docker ──────────────────────────────────
FROM python:3.11-bookworm AS wheel-docker

RUN apt-get update && apt-get -y install libpq-dev && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"

RUN pip install maturin

COPY --from=wren-core-py . /wren-core-py
COPY --from=wren-core . /wren-core
COPY --from=wren-core-base . /wren-core-base

WORKDIR /wren-core-py

# Cache cargo registry and target/ between builds for incremental recompilation
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/wren-core-py/target \
    maturin build --release && \
    mkdir /wheel && cp target/wheels/wren_core_py-*.whl /wheel/


# ── Option B: Use locally cross-compiled Linux wheel ─────────────────────────
FROM python:3.11-bookworm AS wheel-local
COPY --from=wren-core-py target/wheels/ /tmp/wheels/
RUN mkdir /wheel && cp /tmp/wheels/wren_core_py-*linux*.whl /wheel/


# ── Select wheel source ───────────────────────────────────────────────────────
FROM wheel-${WHEEL_SOURCE} AS wheel-final


# ── Python builder ────────────────────────────────────────────────────────────
FROM python:3.11-bookworm AS builder

ARG ENV
ENV ENV=$ENV

RUN apt-get update && apt-get -y install libpq-dev && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1

RUN pip install poetry==1.8.3

WORKDIR /app

# Copy dependency files first so this layer is cached when only source changes
COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/root/.cache/pypoetry/cache \
    --mount=type=cache,target=/root/.cache/pypoetry/artifacts \
    poetry install --without dev --with jupyter --no-root

# Install wheel (either from host or from wheel-docker stage)
COPY --from=wheel-final /wheel/ /tmp/wheels/
RUN poetry run pip install /tmp/wheels/wren_core_py-*.whl

# Install MCP server dependencies
RUN poetry run pip install "mcp[cli]>=1.19.0,<2"

COPY . .


# ── Runtime ───────────────────────────────────────────────────────────────────
FROM python:3.11-slim-bookworm AS runtime

# Install all system dependencies in a single layer
RUN apt-get update \
    && apt-get install -y curl gnupg \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y unixodbc-dev msodbcsql18 \
    && apt-get install -y default-libmysqlclient-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app" \
    REMOTE_FUNCTION_LIST_PATH=/app/resources/function_list \
    REMOTE_WHITE_FUNCTION_LIST_PATH=/app/resources/white_function_list

WORKDIR /app

COPY app ./app/
COPY resources ./resources/
COPY notebooks ./notebooks/
COPY wren/ ./wren/

# Copy MCP server
COPY --from=mcp-server ./app /mcp-server/app/
COPY --from=mcp-server ./mdl.schema.json /mcp-server/
COPY --from=mcp-server ./pyproject.toml /mcp-server/
ARG MCP_SERVER_VERSION=unknown
RUN if [ -n "$MCP_SERVER_VERSION" ]; then \
    sed -i "s/^version = \".*\"/version = \"$MCP_SERVER_VERSION\"/" /mcp-server/pyproject.toml; \
    fi

COPY pyproject.toml poetry.lock ./

# Copy venv from builder (includes all deps + jupyter)
COPY --from=builder /app/.venv ${VIRTUAL_ENV}

# Install Opentelemetry zero-instrumentation python
RUN pip install opentelemetry-distro opentelemetry-exporter-otlp \
    && opentelemetry-bootstrap -a install

# Generate jupyter lab config
RUN jupyter lab --generate-config --allow-root

COPY entrypoint.sh ./
RUN chmod +x ./entrypoint.sh

LABEL org.opencontainers.image.mcp-server.version=${MCP_SERVER_VERSION}

EXPOSE 8000 8888 9000

ENTRYPOINT ["./entrypoint.sh"]
