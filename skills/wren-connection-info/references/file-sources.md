# File-Based Connectors — Required Fields

## Local files (`LOCAL_FILE`)

| Field | Description |
|-------|-------------|
| `url` | Root path to the directory containing data files |
| `format` | File format: `"csv"`, `"parquet"`, `"json"` |

> When running via Docker, the files must be inside the mounted workspace.

---

## Amazon S3 (`S3_FILE`)

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `url` | Root path within the bucket | |
| `format` | File format: `"csv"`, `"parquet"`, `"json"` | |
| `bucket` | S3 bucket name | |
| `region` | AWS region (e.g. `us-east-1`) | |
| `access_key` | AWS access key ID | ✓ |
| `secret_key` | AWS secret access key | ✓ |

---

## MinIO (`MINIO_FILE`)

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `url` | Root path within the bucket | |
| `format` | File format: `"csv"`, `"parquet"`, `"json"` | |
| `endpoint` | MinIO endpoint (e.g. `localhost:9000`) | |
| `bucket` | Bucket name | |
| `access_key` | Access key ID | ✓ |
| `secret_key` | Secret access key | ✓ |
| `ssl_enabled` | Enable SSL (`"true"` / `"false"`) | |

---

## Google Cloud Storage (`GCS_FILE`)

| Field | Description | Sensitive |
|-------|-------------|-----------|
| `url` | Root path within the bucket | |
| `format` | File format: `"csv"`, `"parquet"`, `"json"` | |
| `bucket` | GCS bucket name | |
| `key_id` | Service account key ID | ✓ |
| `secret_key` | Service account secret key | ✓ |
| `credentials` | Base64-encoded service account JSON | ✓ |
