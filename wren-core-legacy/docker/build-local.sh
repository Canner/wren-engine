#!/usr/bin/env bash

set -euo pipefail

SOURCE_DIR="../"

# Retrieve the script directory.
SCRIPT_DIR="${BASH_SOURCE%/*}"
cd ${SCRIPT_DIR}

# Move to the root directory to run maven for current version.
pushd ${SOURCE_DIR}
WREN_VERSION=$(./mvnw --quiet help:evaluate -Dexpression=project.version -DforceStdout)
popd

WORK_DIR="$(mktemp -d)"
cp ${SOURCE_DIR}wren-server/target/wren-server-${WREN_VERSION}-executable.jar ${WORK_DIR}
cp ./entrypoint.sh ${WORK_DIR}

CONTAINER="wren-engine:${WREN_VERSION}"

docker build ${WORK_DIR} --pull --platform linux/amd64 -f Dockerfile -t ${CONTAINER}-amd64 --build-arg "WREN_VERSION=${WREN_VERSION}"
docker build ${WORK_DIR} --pull --platform linux/arm64 -f Dockerfile -t ${CONTAINER}-arm64 --build-arg "WREN_VERSION=${WREN_VERSION}"

rm -r ${WORK_DIR}

docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' ${CONTAINER}-amd64
docker image inspect -f '🚀 Built {{.RepoTags}} {{.Id}}' ${CONTAINER}-arm64
