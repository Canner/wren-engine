---
name: wren-connection-info
description: Reference guide for Wren Engine connection info — explains required fields per data source, sensitive field handling, Docker host hints, and BigQuery credential encoding. Use when the user asks how to configure a data source connection or what fields to fill in.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.4"
---

# Wren Connection Info Reference

This skill answers questions about how to configure connection info for each data source in Wren Engine. Use it to explain required fields, flag sensitive values, and guide the user through any data-source-specific setup steps.

Connection info can **only** be configured through the MCP server Web UI at `http://localhost:9001`. There is no ibis-server API for writing connection info — do not attempt to set it programmatically via API calls, curl, or any other method. Always direct the user to the Web UI to enter or update credentials.

---

## Data source types

| Value | Database |
|-------|----------|
| `POSTGRES` | PostgreSQL |
| `MYSQL` | MySQL / MariaDB |
| `MSSQL` | SQL Server |
| `DUCKDB` | DuckDB |
| `BIGQUERY` | Google BigQuery |
| `SNOWFLAKE` | Snowflake |
| `CLICKHOUSE` | ClickHouse |
| `TRINO` | Trino |
| `ATHENA` | AWS Athena |
| `ORACLE` | Oracle |
| `DATABRICKS` | Databricks |

---

## Required fields per data source

### PostgreSQL / MySQL / MSSQL / ClickHouse / Oracle

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `host` | Hostname or IP | |
| `port` | Port number | |
| `user` | Username | |
| `password` | Password | ✓ |
| `database` | Database name | |

Default ports: PostgreSQL `5432`, MySQL `3306`, MSSQL `1433`, ClickHouse `8123`, Oracle `1521`

### Trino

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `host` | Hostname | |
| `port` | Port (default `8080`) | |
| `user` | Username | |
| `catalog` | Catalog name | |
| `schema` | Schema name | |

### BigQuery

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `project_id` | GCP project ID | |
| `dataset_id` | Dataset name | |
| `credentials_json_string` | Base64-encoded service account JSON | ✓ |

> **BigQuery credentials encoding**: Wren requires the service account JSON as a **base64-encoded string**, not the raw file.
> After downloading `credentials.json` from GCP, run:
> ```bash
> # macOS
> base64 -i credentials.json | tr -d '\n'
> # Linux
> base64 -w 0 credentials.json
> ```
> Paste the output into the `credentials_json_string` field.

### Snowflake

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `user` | Username | |
| `password` | Password | ✓ |
| `account` | Account identifier | |
| `database` | Database name | |
| `sf_schema` | Schema name | |

### DuckDB

| Field | Description |
|-------|-------------|
| `format` | Must be `"duckdb"` |
| `url` | Path to the folder containing the `.duckdb` file |

> When running via Docker, the `.duckdb` file must be inside the mounted workspace (e.g. `/workspace/mydb.duckdb`).

### File-based (S3, Minio, GCS, local file)

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `format` | File format: `"csv"`, `"parquet"`, etc. | |
| `url` | File path or bucket URL | |
| `access_key` | Access key (object storage) | ✓ |
| `secret_key` | Secret key (object storage) | ✓ |

### Athena

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `s3_staging_dir` | S3 staging directory (`s3://bucket/prefix/`) | |
| `region` | AWS region | |
| `aws_access_key_id` | AWS access key ID | ✓ |
| `aws_secret_access_key` | AWS secret access key | ✓ |

### Databricks

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `host` | Workspace hostname | |
| `http_path` | SQL warehouse HTTP path | |
| `access_token` | Personal access token | ✓ |

---

## Docker host hint

If the database runs on the **host machine** and Wren Engine runs inside Docker, `localhost` and `127.0.0.1` cannot reach the host. Use `host.docker.internal` instead:

| Original | Inside Docker |
|----------|--------------|
| `localhost` | `host.docker.internal` |
| `127.0.0.1` | `host.docker.internal` |
| Cloud/remote hostname | No change needed |

The MCP server Web UI (`http://localhost:9001`) shows a hint for this when relevant.

---

## Sensitive fields summary

Never log, display, or pass sensitive values through the AI agent unnecessarily.

| Connector | Sensitive fields |
|-----------|-----------------|
| Postgres / MySQL / MSSQL / ClickHouse / Oracle | `password` |
| BigQuery | `credentials_json_string` |
| Snowflake | `password`, `private_key` |
| Athena | `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token` |
| S3 / Minio / GCS file | `access_key`, `secret_key` |
| Databricks | `access_token`, `client_secret` |
