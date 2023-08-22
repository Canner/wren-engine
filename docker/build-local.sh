#!/usr/bin/env bash

set -euo pipefail

SOURCE_DIR="../"

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ${SOURCE_DIR}
ACCIO_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd

WORK_DIR="$(mktemp -d)"
cp ${SOURCE_DIR}/accio-server/target/accio-server-${ACCIO_VERSION}-executable.jar ${WORK_DIR}
cp ${SOURCE_DIR}/accio-validation/target/accio-cli ${WORK_DIR}


CONTAINER="accio:${ACCIO_VERSION}"

docker build ${WORK_DIR} --pull --platform linux/amd64 -f Dockerfile -t ${CONTAINER}-amd64 --build-arg "ACCIO_VERSION=${ACCIO_VERSION}"
docker build ${WORK_DIR} --pull --platform linux/arm64 -f Dockerfile -t ${CONTAINER}-arm64 --build-arg "ACCIO_VERSION=${ACCIO_VERSION}"

rm -r ${WORK_DIR}

docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' ${CONTAINER}-amd64
docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' ${CONTAINER}-arm64
