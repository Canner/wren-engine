use std::error::Error;
use std::fmt;
use std::sync::Arc;

use base64::prelude::*;
use pyo3::prelude::*;

use wren_core::mdl;
use wren_core::mdl::manifest::Manifest;
use wren_core::mdl::AnalyzedWrenMDL;

#[derive(Debug)]
struct CoreError {
    message: String,
}

impl CoreError {
    fn new(msg: &str) -> CoreError {
        CoreError {
            message: msg.to_string(),
        }
    }
}

impl Error for CoreError {}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<CoreError> for PyErr {
    fn from(err: CoreError) -> PyErr {
        CoreError::new(&err.to_string()).into()
    }
}

#[pyfunction]
fn transform_sql(mdl_base64: &str, sql: &str) -> Result<String, CoreError> {
    let mdl_json_bytes = BASE64_STANDARD
        .decode(mdl_base64)
        .map_err(|e| CoreError::new(&e.to_string()))?;
    let mdl_json = String::from_utf8(mdl_json_bytes).map_err(|e| CoreError::new(&e.to_string()))?;
    let manifest =
        serde_json::from_str::<Manifest>(&mdl_json).map_err(|e| CoreError::new(&e.to_string()))?;
    let analyzed_mdl = AnalyzedWrenMDL::analyze(manifest);
    match mdl::transform_sql(Arc::new(analyzed_mdl), sql) {
        Ok(transformed_sql) => Ok(transformed_sql),
        Err(e) => Err(CoreError::new(&e.to_string())),
    }
}

#[pymodule]
fn modeling_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(transform_sql, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::transform_sql;
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use serde_json::Value;

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
            transform_sql(&mdl_base64, "SELECT * FROM my_catalog.my_schema.customer").unwrap();
        assert_eq!(transformed_sql, "SELECT \"my_catalog\".\"my_schema\".\"customer\".\"custkey\", \"my_catalog\".\"my_schema\".\"customer\".\"name\" FROM (SELECT \"customer\".\"c_custkey\" AS \"custkey\", \"customer\".\"c_name\" AS \"name\" FROM \"customer\") AS \"customer\"");
    }
}
