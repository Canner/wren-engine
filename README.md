# Accio

This project is a metric layer framework which follow Postgres Wire Protocol to communicate with its client.
Its codebase is based on [Trino](https://github.com/trinodb/trino) project and the part of
Postgres Wire Protocol in [CrateDB](https://github.com/crate/crate) project.

# How to build

```shell
mvn clean install -DskipTests
```

# How to Run

## Required Configuration

- `etc/config.properties`

for bigquery settings

```
node.environment=
bigquery.project-id=         # The target BigQuery project
bigquery.credentials-key=    # based 64 credentials key
bigquery.location=           # BigQuery execution region
bigquery.bucket-name=        # GCS bucket name
duckdb.storage.access-key=   # GCS storage access key
duckdb.storage.secret-key=   # GCS storage secret key
accio.file=                  # accio mdl json file path
```

- VM options

```
-Dconfig=etc/config.properties   # the path of config file
```
