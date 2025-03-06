# Ibis Server Traced Metrics

The ibis-server codebase uses OpenTelemetry for tracing. The following spans are traced across different components:

## Rewriter Module
- `transpile` - Internal span for SQL transpilation operations
- `rewrite` - Internal span for SQL rewriting operations
- `extract_manifest` - Internal span for manifest extraction from SQL
- `external_rewrite` - Client span for external engine rewriting operations
- `embedded_rewrite` - Internal span for embedded engine rewriting operations

## Substitute Module
- `substitute` - Internal span for model substitution operations

## Connector Module
- `connector_init` - Internal span for connector initialization
- `connector_query` - Client span for executing queries
- `connector_dry_run` - Client span for dry-running queries
- `describe_sql_for_error_message` - Client span for generating SQL error messages
- `get_schema` - Client span for retrieving schema information
- `duckdb_query` - Internal span for DuckDB queries
- `duckdb_dry_run` - Internal span for DuckDB dry runs

## API Endpoints (v2)
- `v2_query_{data_source}` - Server span for query operations
- `v2_query_{data_source}_dry_run` - Server span for dry run query operations
- `v2_validate_{data_source}` - Server span for validation operations
- `v2_metadata_tables_{data_source}` - Server span for metadata table listing
- `v2_metadata_constraints_{data_source}` - Server span for metadata constraint listing
- `dry_plan` - Server span for dry planning operations
- `v2_dry_plan_{data_source}` - Server span for data source specific dry planning
- `v2_model_substitute_{data_source}` - Server span for model substitution operations

## API Endpoints (v3)
- `v3_query_{data_source}` - Server span for query operations
- `v3_query_{data_source}_dry_run` - Server span for dry run query operations
- `v3_dry_plan_{data_source}` - Server span for data source specific dry planning
- `v3_validate_{data_source}` - Server span for validation operations
- `v3_functions_{data_source}` - Server span for function listing
- `v3_model-substitute_{data_source}` - Server span for model substitution operations

## Utility Functions
- `base64_to_dict` - Internal span for base64 to dictionary conversion
- `to_json` - Internal span for DataFrame to JSON conversion

## Trace Context
- Each endpoint accepts request headers and properly propagates trace context using the `build_context` function.
