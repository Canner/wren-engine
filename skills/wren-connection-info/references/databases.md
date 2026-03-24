# Database Connectors — Required Fields

## PostgreSQL / MySQL / MSSQL / ClickHouse / Oracle / Doris / Redshift

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `host` | Hostname or IP | |
| `port` | Port number | |
| `user` | Username | |
| `password` | Password | ✓ |
| `database` | Database name | |

Default ports: PostgreSQL `5432`, MySQL `3306`, MSSQL `1433`, ClickHouse `8123`, Oracle `1521`, Doris `9030`, Redshift `5439`

---

## Trino

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `host` | Hostname | |
| `port` | Port (default `8080`) | |
| `user` | Username | |
| `catalog` | Catalog name | |
| `schema` | Schema name | |

---

## BigQuery

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `project_id` | GCP project ID | |
| `dataset_id` | Dataset name | |
| `credentials_json_string` | Base64-encoded service account JSON | ✓ |

**BigQuery credentials encoding**: Wren requires the service account JSON as a **base64-encoded string**, not the raw file.
After downloading `credentials.json` from GCP, run:

```bash
# macOS
base64 -i credentials.json | tr -d '\n'
# Linux
base64 -w 0 credentials.json
```

Paste the output into the `credentials_json_string` field.

---

## Snowflake

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `user` | Username | |
| `password` | Password | ✓ |
| `account` | Account identifier | |
| `database` | Database name | |
| `sf_schema` | Schema name | |

---

## DuckDB

| Field | Description |
|-------|-------------|
| `format` | Must be `"duckdb"` |
| `url` | Path to the folder containing the `.duckdb` file |

> When running via Docker, the `.duckdb` file must be inside the mounted workspace (e.g. `/workspace/mydb.duckdb`).

---

## Athena

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `s3_staging_dir` | S3 staging directory (`s3://bucket/prefix/`) | |
| `region` | AWS region | |
| `aws_access_key_id` | AWS access key ID | ✓ |
| `aws_secret_access_key` | AWS secret access key | ✓ |

---

## Databricks (token auth)

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `server_hostname` | Workspace hostname (e.g. `dbc-xxx.cloud.databricks.com`) | |
| `http_path` | SQL warehouse HTTP path (e.g. `/sql/1.0/warehouses/xxx`) | |
| `access_token` | Personal access token | ✓ |

## Databricks (service principal)

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `server_hostname` | Workspace hostname | |
| `http_path` | SQL warehouse HTTP path | |
| `client_id` | OAuth M2M client ID | ✓ |
| `client_secret` | OAuth M2M client secret | ✓ |
| `azure_tenant_id` | Azure AD tenant ID (Azure Databricks only) | |

---

## Spark (Spark Connect)

| Field | Description |
|-------|-------------|
| `host` | Spark Connect server hostname |
| `port` | Spark Connect server port |

> Spark uses the Spark Connect protocol (`sc://host:port`). No authentication fields — access control is managed at the Spark cluster level.
