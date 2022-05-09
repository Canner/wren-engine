# Canner Metric Layer (CML)

This project is a metric layer framework which follow Postgres Wire Protocol to communicate with its client.
Its codebase is based on [Trino](https://github.com/trinodb/trino) project and the part of
Postgres Wire Protocol in [CrateDB](https://github.com/crate/crate) project.

# How to build

```dtd
mvn clean install -DskipTests
```

# How to Run

## Required Configuration

- `etc/config.properties`

```dtd
bigquery.project-id=         # The target bigQuery project
        bigquery.credentials-key=    # based 64 credentials key
        bigquery.location=           # BigQuery execution region
```

- VM options

```dtd
-Dconfig=etc/config.properties   # the path of config file
```