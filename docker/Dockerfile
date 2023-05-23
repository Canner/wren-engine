FROM azul/zulu-openjdk:11.0.14
LABEL maintainer="https://www.canner.io/"
WORKDIR /usr/src/app

ARG GRAPHMDL_VERSION
ENV ENV_GRAPHMDL_VERSION=${GRAPHMDL_VERSION}
COPY graphmdl-server-${GRAPHMDL_VERSION}-executable.jar ./

CMD ["sh", "-c", "java -Dconfig=etc/config.properties -jar graphmdl-server-${ENV_GRAPHMDL_VERSION}-executable.jar"]
