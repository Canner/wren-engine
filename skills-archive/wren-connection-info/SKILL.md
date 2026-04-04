---
name: wren-connection-info
description: "Reference guide for Wren Engine connection info — explains required fields for all 18 supported data sources (PostgreSQL, MySQL, BigQuery, Snowflake, ClickHouse, Trino, DuckDB, Databricks, Spark, Athena, Redshift, Oracle, SQL Server, Apache Doris, S3, GCS, MinIO, local files). Covers sensitive field handling, Docker host hints, and BigQuery credential encoding. Use when the user asks how to configure a data source connection or what fields to fill in."
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.5"
---

# Wren Connection Info Reference

Connection info can **only** be configured through the MCP server Web UI at `http://localhost:9001`. Do not attempt to set it programmatically. Always direct the user to the Web UI.

---

## Supported data sources

| Value | Database | Fields reference |
|-------|----------|-----------------|
| `POSTGRES` | PostgreSQL | [databases.md](references/databases.md) |
| `MYSQL` | MySQL / MariaDB | [databases.md](references/databases.md) |
| `MSSQL` | SQL Server | [databases.md](references/databases.md) |
| `CLICKHOUSE` | ClickHouse | [databases.md](references/databases.md) |
| `ORACLE` | Oracle | [databases.md](references/databases.md) |
| `DORIS` | Apache Doris | [databases.md](references/databases.md) |
| `REDSHIFT` | Amazon Redshift | [databases.md](references/databases.md) |
| `TRINO` | Trino | [databases.md](references/databases.md) |
| `BIGQUERY` | Google BigQuery | [databases.md](references/databases.md) |
| `SNOWFLAKE` | Snowflake | [databases.md](references/databases.md) |
| `DUCKDB` | DuckDB | [databases.md](references/databases.md) |
| `ATHENA` | AWS Athena | [databases.md](references/databases.md) |
| `DATABRICKS` | Databricks | [databases.md](references/databases.md) |
| `SPARK` | Apache Spark | [databases.md](references/databases.md) |
| `S3_FILE` | Amazon S3 | [file-sources.md](references/file-sources.md) |
| `GCS_FILE` | Google Cloud Storage | [file-sources.md](references/file-sources.md) |
| `MINIO_FILE` | MinIO | [file-sources.md](references/file-sources.md) |
| `LOCAL_FILE` | Local files | [file-sources.md](references/file-sources.md) |

Read the linked reference file for the user's data source to get required fields, default ports, and setup notes.

---

## Common patterns

Most database connectors need: `host`, `port`, `user`, `password`, `database`.

Exceptions:
- **BigQuery** — uses `project_id`, `dataset_id`, `credentials` (base64-encoded). See [databases.md](references/databases.md) for encoding instructions.
- **Snowflake** — uses `account` instead of `host`, plus `schema`.
- **Trino** — needs `catalog` and `schema` instead of `database`.
- **Databricks** — uses `serverHostname`, `httpPath`, `accessToken` (or service principal with `clientId`, `clientSecret`).
- **Spark** — only `host` and `port` (Spark Connect protocol, no auth fields).
- **File sources** — use `url`, `format`, plus bucket/credentials. See [file-sources.md](references/file-sources.md).

---

## Docker host hint

If the database runs on the **host machine** and Wren Engine runs inside Docker, `localhost` cannot reach the host. Use `host.docker.internal` instead:

| Original | Inside Docker |
|----------|--------------|
| `localhost` | `host.docker.internal` |
| `127.0.0.1` | `host.docker.internal` |
| Cloud/remote hostname | No change needed |

---

## Sensitive fields

Never log, display, or pass sensitive values through the AI agent unnecessarily.

| Connector | Sensitive fields |
|-----------|-----------------|
| Postgres / MySQL / MSSQL / ClickHouse / Oracle / Doris / Redshift | `password` |
| BigQuery | `credentials` |
| Snowflake | `password` |
| Athena | `aws_access_key_id`, `aws_secret_access_key` |
| Databricks (token) | `accessToken` |
| Databricks (service principal) | `clientId`, `clientSecret` |
| S3 / MinIO | `access_key`, `secret_key` |
| GCS | `key_id`, `secret_key`, `credentials` |
| Trino / Spark / Local files | (none) |
