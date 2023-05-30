FROM azul/zulu-openjdk:11.0.14
LABEL maintainer="https://www.canner.io/"
WORKDIR /usr/src/app

RUN \
    apt update && \
    apt -y install curl gpg lsb-release && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list && \
    apt update && \
    apt -y install postgresql-client-13

ARG GRAPHMDL_VERSION
ENV ENV_GRAPHMDL_VERSION=${GRAPHMDL_VERSION}
COPY graphmdl-server-${GRAPHMDL_VERSION}-executable.jar ./

CMD ["sh", "-c", "java -Dconfig=etc/config.properties -jar graphmdl-server-${ENV_GRAPHMDL_VERSION}-executable.jar"]
