---
name: wren-connection-info
description: Set up data source type and connection credentials for Wren Engine. Use at the start of any workflow that connects to a database — produces either a connectionFilePath (secure, default) or an inline connectionInfo dict (opt-in for testing). Trigger before generate-mdl, wren-project, or any ibis-server API call that needs credentials.
license: Apache-2.0
metadata:
  author: wren-engine
  version: "1.2"
---

# Wren Connection Info

Sets up the data source type and credentials before any workflow that queries a database.
Find the specification of the connection info format in the `model` section of [API reference](https://docs.getwren.ai/oss/wren_engine_api).

---

## Step 1 — Choose data source

Ask the user for their **data source type**:

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

> **Docker note**: If the database runs on the host machine and ibis-server runs inside Docker, replace `localhost` / `127.0.0.1` with `host.docker.internal` in the host field.

---

## Step 2 — Choose connection mode

Two modes are supported. Ask the user which they prefer, or infer from context.

The chosen mode is recorded as `connection_mode` in `wren_project.yml` so every subsequent workflow knows how to handle credentials.

### Mode A — Secure (default, recommended for production)

`connection_mode: security` in `wren_project.yml`.

The LLM never handles sensitive values. ibis-server reads the connection file directly.

Use this mode by default unless the user explicitly says they are in a test/development environment and willing to share credentials.

**When `connection_mode: security` is in effect** (either set explicitly or because the field is absent):
- **Never** read `connection.yml` or `target/connection.json` without first asking the user for permission.
- **Never** display or echo the contents of those files.
- If debugging requires connection info, ask the user to share only non-sensitive fields (e.g. `host`, `port`, `database`, `user`) — never passwords, tokens, or keys.

### Mode B — Inline (opt-in, testing only)

`connection_mode: inline` in `wren_project.yml`.

> **How to opt in**: The user must say something like "I'm just testing, you can use my credentials" or "it's a dev environment, here are my connection details". Do not assume this mode.

In this mode, ask for all fields including sensitive ones and assemble an inline `connectionInfo` dict.

---

## Step 3 — Gather credentials

Ask for the fields required for the chosen data source. Sensitive fields (marked **secret**) must **never** be filled in by the LLM in Mode A — leave them as `# TODO` comments.

### PostgreSQL / MySQL / MSSQL / ClickHouse / Oracle

```
host:     <hostname or IP>
port:     <port>
user:     <username>
password: <SECRET>
database: <database name>
```

Default ports: PostgreSQL `5432`, MySQL `3306`, MSSQL `1433`, ClickHouse `8123`, Oracle `1521`

### Trino

```
host:    <hostname>
port:    <port, default 8080>
user:    <username>
catalog: <catalog name>
schema:  <schema name>
```

### BigQuery

```
project_id:              <GCP project ID>
dataset_id:              <dataset name>
credentials_json_string: <SECRET — base64-encoded service account JSON>
```

> **BigQuery credentials**: Wren requires the service account JSON as a **base64-encoded string**, not the raw file.
> After downloading `credentials.json` from GCP, run:
> ```bash
> base64 -i credentials.json | tr -d '\n'
> ```
> Paste the output as the value of `credentials_json_string`.
> On Linux: `base64 -w 0 credentials.json`

### Snowflake

```
user:      <username>
password:  <SECRET>
account:   <account identifier>
database:  <database name>
sf_schema: <schema name>
```

### File based (S3, Minio, GCS, local file)
For the object storage connectors (S3, Minio, GCS) and local file sources, use this format:
```
format: <FILE_FORMAT>   # e.g. "csv", "parquet"
url:    <file path or bucket URL>
```
If credentials are needed (e.g. S3 access key and secret), include these fields as well:
```
access_key: <SECRET>
secret_key: <SECRET>
```

### DuckDB
It leverages the same connection info format as file-based sources, but with `format: duckdb` to indicate that it's a DuckDB data source rather than a generic file source.
```
format: duckdb
url: <path to folder containing .duckdb file>
```

### Athena

```
s3_staging_dir:        <s3://bucket/prefix/>
region:                <AWS region>
aws_access_key_id:     <SECRET>
aws_secret_access_key: <SECRET>
```

### Databricks

```
host:         <workspace hostname>
http_path:    <SQL warehouse HTTP path>
access_token: <SECRET>
```

**Sensitive fields by connector** — LLM must never populate these in Mode A:

| Connector | Sensitive fields |
|-----------|-----------------|
| Postgres / MySQL / MSSQL / ClickHouse / Trino / Oracle | `password` |
| BigQuery | `credentials_json_string` |
| Snowflake | `password`, `private_key` |
| Athena | `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `web_identity_token` |
| S3 / Minio / GCS file | `access_key`, `secret_key` |
| Databricks | `access_token`, `client_secret` |
| Canner | `pat` |

---

## Step 4 — Produce output

### Mode A output

Write `<project_dir>/connection.yml` with non-sensitive fields filled in and sensitive fields as `# TODO` comments:

```yaml
# Example: PostgreSQL
host: my-db.example.com
port: 5432
user: my_user
password:   # TODO: fill in your database password
database: my_db
```

Then instruct the user:
> Please fill in the sensitive fields in `connection.yml`, then let me know when done.

Wait for confirmation, then build `target/connection.json`:

```bash
python -c "
import yaml, json, pathlib
p = pathlib.Path('connection.yml')
d = yaml.safe_load(p.read_text())
pathlib.Path('target').mkdir(exist_ok=True)
json.dump(d, open('target/connection.json', 'w'))
"
```

**Do NOT read or display the contents of `target/connection.json` after building.**

> **Server requirement:** ibis-server must have the `CONNECTION_FILE_ROOT` environment variable set to the directory containing `target/connection.json`. When running via Docker (the standard deployment), the workspace is mounted at `/workspace` and `CONNECTION_FILE_ROOT=/workspace` is set by default. For local dev, set `CONNECTION_FILE_ROOT` to the project root before starting ibis-server.

Provide to the calling workflow:
- `connectionFilePath`: absolute path to `target/connection.json` — use the **container-internal** path (e.g. `/workspace/target/connection.json`) when ibis-server runs in Docker, or the host path for local dev
- `data_source`: the data source type string (e.g. `"POSTGRES"`)
- `connection_mode`: `"security"` — record this in `wren_project.yml`

### Mode B output

Assemble the inline dict directly. Provide to the calling workflow:
- `connectionInfo`: camelCase JSON dict (see [Field mapping](#field-mapping) below)
- `data_source`: the data source type string
- `connection_mode`: `"inline"` — record this in `wren_project.yml`

---

## Field mapping

When converting `connection.yml` to `target/connection.json`, rename these snake_case keys to camelCase:

| YAML (snake_case) | JSON (camelCase) |
|-------------------|-----------------|
| `project_id` | `projectId` |
| `dataset_id` | `datasetId` |
| `credentials_json_string` | `credentialsJsonString` |
| `sf_schema` | `sfSchema` |
| `ssl_mode` | `sslMode` |
| `ssl_ca` | `sslCA` |
| `connection_url` | `connectionUrl` |
| `http_path` | `httpPath` |
| `access_token` | `accessToken` |
| `s3_staging_dir` | `s3StagingDir` |
| `aws_access_key_id` | `awsAccessKeyId` |
| `aws_secret_access_key` | `awsSecretAccessKey` |

Fields without underscores (`host`, `port`, `user`, `password`, `database`, `account`, `url`, `catalog`, `schema`, `region`) remain unchanged. All other snake_case fields should be converted to camelCase for JSON.

---

## Using connection info in API calls

After this skill completes, use the output in ibis-server API calls:

**Mode A (file path):**
```json
{
  "connectionFilePath": "/abs/path/to/target/connection.json",
  "manifestStr": "...",
  "sql": "..."
}
```

**Mode B (inline):**
```json
{
  "connectionInfo": { "host": "...", "port": "5432", ... },
  "manifestStr": "...",
  "sql": "..."
}
```
