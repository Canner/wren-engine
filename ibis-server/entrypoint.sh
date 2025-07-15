#!/bin/bash

# Check if the first argument is 'jupyter'
if [[ "$1" == "jupyter" ]]; then
    # If only 'jupyter' is passed, start jupyter lab with default configuration
    if [[ $# -eq 1 ]]; then
        echo "Starting Jupyter Lab..."
        exec jupyter lab \
            --ip=0.0.0.0 \
            --port=8888 \
            --no-browser \
            --allow-root \
            --ServerApp.token="" \
            --ServerApp.password="" \
            --ServerApp.allow_origin="*" \
            --ServerApp.base_url="/lab" \
            --ServerApp.default_url="/lab/tree/notebooks/demo.ipynb" \
            --ServerApp.iopub_data_rate_limit=1000000000 \
            --ServerApp.websocket_ping_timeout=30000 \
            --ServerApp.websocket_ping_interval=30000
    else
        # If jupyter with additional arguments, pass all arguments to jupyter
        echo "Executing: $*"
        exec "$@"
    fi
fi

# Default behavior: start ibis-server
echo "Starting Ibis Server..."

if [[ -z "${WREN_NUM_WORKERS}" ]]; then
    echo "WREN_NUM_WORKERS is not set. Using default value of 2."
    WREN_NUM_WORKERS=2
else
    WREN_NUM_WORKERS=${WREN_NUM_WORKERS}
fi

echo "Number of workers: ${WREN_NUM_WORKERS}"

# Determine the command prefix based on OTLP_ENABLED
if [[ "${OTLP_ENABLED}" == "true" ]]; then
    CMD_PREFIX="opentelemetry-instrument"
else
    CMD_PREFIX=""
fi

# Start the WrenUvicornWorker with the specified configuration
${CMD_PREFIX} gunicorn app.main:app --bind 0.0.0.0:8000 \
    -k app.worker.WrenUvicornWorker \
    --workers ${WREN_NUM_WORKERS} \
    --max-requests 1000 \
    --max-requests-jitter 100 \
    --timeout 300 \
    --graceful-timeout 60
