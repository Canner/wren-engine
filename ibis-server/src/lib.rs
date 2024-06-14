use std::sync::Arc;

use base64::prelude::*;
use pyo3::prelude::*;

use wrapper::{ManifestBuilderWrapper, ManifestWrapper};
use wren_core::mdl;
use wren_core::mdl::AnalyzedWrenMDL;
use wren_core::mdl::builder::ManifestBuilder;
use wren_core::mdl::manifest::Manifest;

use crate::errors::CoreError;

mod errors;
mod wrapper;

#[pyfunction]
fn builder_from_base64(mdl_base64: &str) -> Result<ManifestBuilderWrapper, CoreError> {
    let mdl_json_bytes = BASE64_STANDARD.decode(mdl_base64).map_err(CoreError::from)?;
    let mdl_json = String::from_utf8(mdl_json_bytes).map_err(CoreError::from)?;
    let manifest = serde_json::from_str::<Manifest>(&mdl_json)?;
    Ok(ManifestBuilder::from_manifest(manifest).into())
}

#[pyfunction]
fn transform_sql(manifest: &ManifestWrapper, sql: &str) -> Result<String, CoreError> {
    let analyzed_mdl = AnalyzedWrenMDL::analyze(manifest.into());
    match mdl::transform_sql(Arc::new(analyzed_mdl), sql) {
        Ok(transformed_sql) => Ok(transformed_sql),
        Err(e) => Err(CoreError::new(&e.to_string())),
    }
}

#[pymodule]
#[pyo3(name = "modeling_core")]
fn modeling_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(builder_from_base64, m)?)?;
    m.add_function(wrap_pyfunction!(transform_sql, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use serde_json::Value;

    use crate::{builder_from_base64, transform_sql};

    #[test]
    fn test_transform_sql() {
        let data = r#"
        {
            "catalog": "my_catalog",
            "schema": "my_schema",
            "models": [
                {
                    "name": "customer",
                    "tableReference": "main.customer",
                    "columns": [
                        {"name": "custkey", "expression": "c_custkey", "type": "integer"},
                        {"name": "name", "expression": "c_name", "type": "varchar"}
                    ],
                    "primaryKey": "custkey"
                }
            ]
        }"#;
        let v: Value = serde_json::from_str(data).unwrap();
        let mdl_base64: String = BASE64_STANDARD.encode(v.to_string().as_bytes());
        let manifest_builder = builder_from_base64(&mdl_base64).unwrap();
        let transformed_sql = transform_sql(
            &manifest_builder.build().unwrap(),
            "SELECT * FROM my_catalog.my_schema.customer",
        )
        .unwrap();
        assert_eq!(transformed_sql, "SELECT \"my_catalog\".\"my_schema\".\"customer\".\"custkey\", \"my_catalog\".\"my_schema\".\"customer\".\"name\" FROM (SELECT \"customer\".\"c_custkey\" AS \"custkey\", \"customer\".\"c_name\" AS \"name\" FROM \"customer\") AS \"customer\"");
    }
}
