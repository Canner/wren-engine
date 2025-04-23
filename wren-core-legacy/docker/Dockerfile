FROM eclipse-temurin:21
LABEL maintainer="https://www.canner.io/"
WORKDIR /usr/src/app

RUN \
    apt update && \
    apt -y install curl gpg lsb-release && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list && \
    apt update && \
    apt -y install postgresql-client-13

ARG WREN_VERSION
ENV ENV_WREN_VERSION=${WREN_VERSION}
ENV WREN_JAR=wren-server-${ENV_WREN_VERSION}-executable.jar
COPY ${WREN_JAR} ./

COPY entrypoint.sh ./
RUN chmod +x ./entrypoint.sh

CMD ./entrypoint.sh ${WREN_JAR} ${MAX_HEAP_SIZE} ${MIN_HEAP_SIZE}
