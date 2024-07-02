use std::sync::Arc;

use base64::prelude::*;
use pyo3::prelude::*;

use wren_core::mdl;
use wren_core::mdl::manifest::Manifest;
use wren_core::mdl::AnalyzedWrenMDL;

use crate::errors::CoreError;

mod errors;

#[pyfunction]
fn transform_sql(mdl_base64: &str, sql: &str) -> Result<String, CoreError> {
    let mdl_json_bytes = BASE64_STANDARD
        .decode(mdl_base64)
        .map_err(CoreError::from)?;
    let mdl_json = String::from_utf8(mdl_json_bytes).map_err(CoreError::from)?;
    let manifest = serde_json::from_str::<Manifest>(&mdl_json)?;

    let Ok(analyzed_mdl) = AnalyzedWrenMDL::analyze(manifest) else {
        return Err(CoreError::new("Failed to analyze manifest"));
    };
    match mdl::transform_sql(Arc::new(analyzed_mdl), sql) {
        Ok(transformed_sql) => Ok(transformed_sql),
        Err(e) => Err(CoreError::new(&e.to_string())),
    }
}

#[pymodule]
#[pyo3(name = "wren_core")]
fn wren_core_wrapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(transform_sql, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use serde_json::Value;

    use crate::transform_sql;

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
        let transformed_sql =
            transform_sql(&mdl_base64, "SELECT * FROM my_catalog.my_schema.customer")
                .unwrap();
        assert_eq!(
            transformed_sql,
            r#"SELECT customer.custkey, customer."name" FROM (SELECT customer.custkey, customer."name" FROM (SELECT main.customer.c_custkey AS custkey, main.customer.c_name AS "name" FROM main.customer) AS customer) AS customer"#
        );
    }
}
