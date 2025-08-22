use crate::errors::CoreError;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use pyo3::pyfunction;

pub use wren_core_base::mdl::*;

/// Convert a manifest to a JSON string and then encode it as base64.
#[pyfunction]
pub fn to_json_base64(mdl: Manifest) -> Result<String, CoreError> {
    let mdl_json = serde_json::to_string(&mdl)?;
    let mdl_base64 = BASE64_STANDARD.encode(mdl_json.as_bytes());
    Ok(mdl_base64)
}

#[pyfunction]
/// Convert a base64 encoded JSON string to a manifest object.
pub fn to_manifest(mdl_base64: &str) -> Result<Manifest, CoreError> {
    let decoded_bytes = BASE64_STANDARD.decode(mdl_base64)?;
    let mdl_json = String::from_utf8(decoded_bytes)?;
    let manifest = serde_json::from_str::<Manifest>(&mdl_json)?;
    Ok(manifest)
}

/// Check if the MDL can be used by the v2 wren core. If there are any access controls rules,
/// the MDL should be used by the v3 wren core only.
#[pyfunction]
pub fn is_backward_compatible(mdl_base64: &str) -> Result<bool, CoreError> {
    let manifest = to_manifest(mdl_base64)?;
    let ralc_exist = manifest
        .models
        .iter()
        .all(|model| model.row_level_access_controls().is_empty());
    let clac_exist = manifest.models.iter().all(|model| {
        model
            .columns
            .iter()
            .all(|column| column.column_level_access_control().is_none())
    });
    Ok(ralc_exist && clac_exist)
}

#[cfg(test)]
mod tests {
    use crate::manifest::{to_json_base64, to_manifest, Manifest};
    use std::sync::Arc;
    use wren_core::mdl::manifest::DataSource::BigQuery;
    use wren_core::mdl::manifest::Model;

    #[test]
    fn test_manifest_to_json_base64() {
        let py_manifest = Manifest {
            catalog: "catalog".to_string(),
            schema: "schema".to_string(),
            models: vec![
                Arc::from(Model {
                    name: "model_1".to_string(),
                    ref_sql: "SELECT * FROM table".to_string().into(),
                    base_object: None,
                    table_reference: None,
                    columns: vec![],
                    primary_key: None,
                    cached: false,
                    refresh_time: None,
                    row_level_access_controls: vec![],
                }),
                Arc::from(Model {
                    name: "model_2".to_string(),
                    ref_sql: None,
                    base_object: None,
                    table_reference: "catalog.schema.table".to_string().into(),
                    columns: vec![],
                    primary_key: None,
                    cached: false,
                    refresh_time: None,
                    row_level_access_controls: vec![],
                }),
            ],
            relationships: vec![],
            metrics: vec![],
            views: vec![],
            data_source: Some(BigQuery),
        };
        let base64_str = to_json_base64(py_manifest).unwrap();
        let manifest = to_manifest(&base64_str).unwrap();
        assert_eq!(manifest.catalog, "catalog");
        assert_eq!(manifest.schema, "schema");
        assert_eq!(manifest.models.len(), 2);
        assert_eq!(manifest.models[0].name, "model_1");
        assert_eq!(
            manifest.models[0].ref_sql,
            Some("SELECT * FROM table".to_string())
        );
        assert_eq!(manifest.models[1].name(), "model_2");
        assert_eq!(manifest.models[1].table_reference(), "catalog.schema.table");
        assert_eq!(manifest.data_source, Some(BigQuery));
    }
}
