#!/bin/bash

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
