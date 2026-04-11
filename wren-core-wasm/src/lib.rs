//! # wren-core-wasm
//!
//! Wren Engine compiled to WebAssembly for browser-native analytics.
//!
//! This crate provides a WASM-compatible version of the Wren Engine that runs
//! entirely in the browser. It uses **upstream DataFusion** (not the Canner fork)
//! because the WASM version executes queries directly via DataFusion — no SQL
//! unparser or dialect transpilation is needed.
//!
//! ## Architecture
//!
//! ```text
//! JS (browser)
//!   │
//!   ├── loadMDL(json)          → WrenEngine stores manifest
//!   ├── registerParquet(bytes) → Arrow RecordBatch → DataFusion MemTable
//!   └── query(sql)             → DataFusion executes → JSON result
//! ```
//!
//! ## Milestone Roadmap
//!
//! - **M1**: DataFusion WASM compilation + in-memory query (this milestone)
//! - **M2**: Parquet file upload + query from browser
//! - **M3**: wren-core semantic layer (MDL plan rewriting)
//! - **M4**: npm package + TypeScript API wrapper

use wasm_bindgen::prelude::*;

// wasm-bindgen-test macros are used in the test module below

/// Wren Engine WASM instance.
///
/// Holds a DataFusion SessionContext and (in M3+) an AnalyzedWrenMDL.
/// All query execution happens in-browser via DataFusion.
#[wasm_bindgen]
pub struct WrenEngine {
    ctx: datafusion::execution::context::SessionContext,
}

#[wasm_bindgen]
impl WrenEngine {
    /// Initialize a new WrenEngine instance.
    ///
    /// Creates a DataFusion SessionContext with default configuration
    /// suitable for single-threaded WASM execution.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<WrenEngine, JsError> {
        // Configure DataFusion for single-threaded WASM environment
        let config = datafusion::execution::context::SessionConfig::new()
            .with_target_partitions(1); // Single-threaded in WASM

        let ctx = datafusion::execution::context::SessionContext::new_with_config(config);

        Ok(WrenEngine { ctx })
    }

    /// Register an in-memory table from a JSON array of objects.
    ///
    /// This is a convenience method for M1 testing. In M2+, use
    /// `register_parquet` to load Parquet files from the browser.
    ///
    /// # Arguments
    /// * `table_name` - Name to register the table under
    /// * `json_data` - JSON string: array of objects, e.g. `[{"a":1,"b":"x"},...]`
    #[wasm_bindgen(js_name = registerJson)]
    pub async fn register_json(
        &self,
        table_name: &str,
        json_data: &str,
    ) -> Result<(), JsError> {
        use arrow::json::reader::infer_json_schema;
        use arrow::json::ReaderBuilder;
        use datafusion::datasource::MemTable;
        use std::io::BufReader;
        use std::sync::Arc;

        // Arrow JSON reader expects NDJSON (one object per line), not a JSON array.
        // Convert JSON array to NDJSON format.
        let ndjson = json_array_to_ndjson(json_data)?;

        // Infer schema from NDJSON data
        let buf_reader = BufReader::new(ndjson.as_bytes());
        let (schema, _) = infer_json_schema(buf_reader, None)
            .map_err(|e| JsError::new(&format!("Failed to infer JSON schema: {e}")))?;

        // Parse NDJSON into Arrow RecordBatch
        let buf_reader = BufReader::new(ndjson.as_bytes());
        let reader = ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(8192)
            .build(buf_reader)
            .map_err(|e| JsError::new(&format!("Failed to parse JSON: {e}")))?;

        let batches: Vec<_> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| JsError::new(&format!("Failed to read JSON batches: {e}")))?;

        if batches.is_empty() {
            return Err(JsError::new("No data in JSON input"));
        }

        let schema = batches[0].schema();
        let table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| JsError::new(&format!("Failed to create table: {e}")))?;

        self.ctx
            .register_table(table_name, Arc::new(table))
            .map_err(|e| JsError::new(&format!("Failed to register table: {e}")))?;

        Ok(())
    }

    /// Register a Parquet file from bytes uploaded via JS.
    ///
    /// Reads the Parquet data into Arrow RecordBatches and registers as a MemTable.
    /// The JS side should pass the file contents as a `Uint8Array`.
    #[wasm_bindgen(js_name = registerParquet)]
    pub async fn register_parquet(
        &self,
        table_name: &str,
        data: &[u8],
    ) -> Result<(), JsError> {
        use datafusion::datasource::MemTable;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::sync::Arc;

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data.to_vec()))
            .map_err(|e| JsError::new(&format!("Failed to open Parquet: {e}")))?;

        let schema = builder.schema().clone();

        let reader = builder
            .build()
            .map_err(|e| JsError::new(&format!("Failed to build Parquet reader: {e}")))?;

        let batches: Vec<_> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| JsError::new(&format!("Failed to read Parquet batches: {e}")))?;

        if batches.is_empty() {
            return Err(JsError::new("No data in Parquet file"));
        }

        let table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| JsError::new(&format!("Failed to create table: {e}")))?;

        self.ctx
            .register_table(table_name, Arc::new(table))
            .map_err(|e| JsError::new(&format!("Failed to register table: {e}")))?;

        Ok(())
    }

    /// Load an MDL (Modeling Definition Language) manifest.
    ///
    /// Parses the MDL JSON, builds the semantic layer (AnalyzedWrenMDL),
    /// and reconfigures the SessionContext with Wren analyzer rules in
    /// LocalRuntime mode (direct DataFusion execution, no SQL generation).
    ///
    /// Physical tables (registered via `registerParquet`/`registerJson`) are
    /// mapped to MDL models through each model's `tableReference`. Register
    /// the backing tables before calling this method.
    ///
    /// After loading, queries can reference MDL model names as table names.
    #[wasm_bindgen(js_name = loadMDL)]
    pub async fn load_mdl(&mut self, mdl_json: &str) -> Result<(), JsError> {
        use std::collections::HashMap;
        use std::sync::Arc;
        use wren_core::mdl::context::{apply_wren_on_ctx, Mode};
        use wren_core::mdl::AnalyzedWrenMDL;
        use wren_core_base::mdl::manifest::Manifest;

        let manifest: Manifest = serde_json::from_str(mdl_json)
            .map_err(|e| JsError::new(&format!("Failed to parse MDL JSON: {e}")))?;

        // Collect existing physical tables from the current context and map them
        // by the table reference names used in the MDL models.
        // The model's tableReference (e.g., "orders" or "schema.orders") is resolved
        // to find the matching physical table in the default DataFusion catalog.
        let mut register_tables: HashMap<
            String,
            Arc<dyn datafusion::datasource::TableProvider>,
        > = HashMap::new();

        let default_catalog = "datafusion";
        let default_schema = "public";

        // Determine data loading strategy based on data source and tableReference format.
        //
        // URL table mode (DataFusion reads Parquet directly via HTTP range requests):
        //   - local_file with http(s):// tableReferences (rewritten by JS for WASM)
        //   - s3, gcs, minio with native URLs (s3://, gs://, etc.)
        //
        // Pre-register mode (tables already loaded via registerParquet/registerJson):
        //   - local_file with local paths (not accessible in WASM, must registerParquet first)
        //   - no dataSource / other data sources (not supported for URL table in WASM)
        let use_url_tables = manifest.models.iter().any(|m| {
            let raw = m.table_reference();
            let url_str = raw.trim_matches('"');
            // Has a URL scheme → URL table mode
            url::Url::parse(url_str).is_ok()
        });

        let analyzed_mdl = if use_url_tables {
                // URL table mode: register object stores for each URL scheme+origin,
                // then DataFusion's ListingTable reads Parquet via range requests.
                let mut registered_origins = std::collections::HashSet::new();
                for model in &manifest.models {
                    let raw = model.table_reference();
                    let url_str = raw.trim_matches('"');
                    if let Ok(parsed) = url::Url::parse(url_str) {
                        let scheme = parsed.scheme();
                        // Only HTTP(S) needs explicit HttpStore registration.
                        // S3/GCS/MinIO stores should be pre-registered or use
                        // DataFusion's built-in object_store integration.
                        if scheme == "http" || scheme == "https" {
                            let origin = parsed.origin().unicode_serialization();
                            if registered_origins.insert(origin.clone()) {
                                let http_store = object_store::http::HttpBuilder::new()
                                    .with_url(&origin)
                                    .build()
                                    .map_err(|e| JsError::new(&format!("Failed to create HTTP store for {origin}: {e}")))?;
                                let base_url = url::Url::parse(&format!("{origin}/"))
                                    .map_err(|e| JsError::new(&format!("Invalid base URL: {e}")))?;
                                self.ctx.register_object_store(&base_url, Arc::new(http_store));
                            }
                        }
                    }
                }

                Arc::new(
                    AnalyzedWrenMDL::analyze_with_url_tables(manifest, &self.ctx).await
                        .map_err(|e| JsError::new(&format!("Failed to analyze MDL with URL tables: {e}")))?,
                )
        } else {
                for model in &manifest.models {
                    let table_ref_str = model.table_reference();
                    if table_ref_str.is_empty() {
                        continue;
                    }

                    // Parse the table reference to extract the table name
                    let table_ref = datafusion::sql::TableReference::from(table_ref_str);
                    let table_name = table_ref.table();

                    // Look up the physical table in the default catalog/schema
                    if let Some(catalog_provider) = self.ctx.catalog(default_catalog) {
                        if let Some(schema_provider) = catalog_provider.schema(default_schema) {
                            if let Ok(Some(table)) = schema_provider.table(table_name).await {
                                // Register with fully qualified name so ModelGenerationRule can find it
                                let qualified_name = format!(
                                    "{}.{}.{}",
                                    default_catalog, default_schema, table_name
                                );
                                register_tables.insert(qualified_name, table);
                            }
                        }
                    }
                }

                Arc::new(
                    AnalyzedWrenMDL::analyze_with_tables(manifest, register_tables)
                        .map_err(|e| JsError::new(&format!("Failed to analyze MDL: {e}")))?,
                )
        };

        let properties: Arc<HashMap<String, Option<String>>> =
            Arc::new(HashMap::new());

        let new_ctx = apply_wren_on_ctx(
            &self.ctx,
            analyzed_mdl,
            properties,
            Mode::LocalRuntime,
        )
        .await
        .map_err(|e| JsError::new(&format!("Failed to apply MDL rules: {e}")))?;

        self.ctx = new_ctx;
        Ok(())
    }

    /// Execute a SQL query and return results as a JSON string.
    ///
    /// Returns a JSON array of objects, e.g. `[{"count":42,"avg":3.14},...]`
    #[wasm_bindgen]
    pub async fn query(&self, sql: &str) -> Result<String, JsError> {
        use arrow::json::writer::JsonArray;
        use arrow::json::WriterBuilder;

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| JsError::new(&format!("SQL error: {e}")))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| JsError::new(&format!("Execution error: {e}")))?;

        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(false)
            .build::<_, JsonArray>(&mut buf);

        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| JsError::new(&format!("JSON serialization error: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| JsError::new(&format!("JSON writer finish error: {e}")))?;

        String::from_utf8(buf)
            .map_err(|e| JsError::new(&format!("UTF-8 encoding error: {e}")))
    }
}

/// Convert a JSON array string to NDJSON (one object per line).
/// Arrow's JSON reader expects NDJSON format.
fn json_array_to_ndjson(json_data: &str) -> Result<String, JsError> {
    let parsed: serde_json::Value = serde_json::from_str(json_data)
        .map_err(|e| JsError::new(&format!("Invalid JSON: {e}")))?;

    match parsed {
        serde_json::Value::Array(arr) => {
            let lines: Result<Vec<String>, _> = arr
                .iter()
                .map(|v| serde_json::to_string(v))
                .collect();
            lines
                .map(|l| l.join("\n"))
                .map_err(|e| JsError::new(&format!("JSON serialization error: {e}")))
        }
        serde_json::Value::Object(_) => {
            // Already a single object, return as-is
            Ok(json_data.to_string())
        }
        _ => Err(JsError::new("Expected JSON array or object")),
    }
}

impl Default for WrenEngine {
    fn default() -> Self {
        Self::new().expect("Failed to create default WrenEngine")
    }
}

// =============================================================================
// Tests (run via wasm-bindgen-test in browser/node)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    // Configure tests to run in Node.js (no browser needed for CI)
    wasm_bindgen_test_configure!(run_in_node_experimental);

    #[wasm_bindgen_test]
    async fn test_basic_query() {
        let engine = WrenEngine::new().unwrap();

        let json_data = r#"[
            {"id": 1, "name": "Alice", "amount": 100.0},
            {"id": 2, "name": "Bob", "amount": 200.0},
            {"id": 3, "name": "Charlie", "amount": 150.0}
        ]"#;

        engine
            .register_json("test_table", json_data)
            .await
            .unwrap();

        let result = engine
            .query("SELECT count(*) as cnt, avg(amount) as avg_amount FROM test_table")
            .await
            .unwrap();

        // Parse and verify
        let rows: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["cnt"], 3);
    }
}
