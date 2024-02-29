FROM azul/zulu-openjdk:11.0.22
LABEL maintainer="https://www.canner.io/"
WORKDIR /usr/src/app

RUN \
    apt update && \
    apt -y install curl gpg lsb-release && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list && \
    apt update && \
    apt -y install postgresql-client-13

ARG ACCIO_VERSION
ENV ENV_ACCIO_VERSION=${ACCIO_VERSION}
COPY accio-server-${ACCIO_VERSION}-executable.jar ./

CMD ["sh", "-c", "java -Dconfig=etc/config.properties -jar accio-server-${ENV_ACCIO_VERSION}-executable.jar"]
