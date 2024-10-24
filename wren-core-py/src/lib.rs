use std::sync::Arc;

use base64::prelude::*;
use pyo3::prelude::*;

use crate::errors::CoreError;
use crate::remote_functions::RemoteFunction;
use log::debug;
use wren_core::mdl;
use wren_core::mdl::manifest::Manifest;
use wren_core::mdl::AnalyzedWrenMDL;

mod errors;
mod remote_functions;

#[pyfunction]
fn transform_sql(
    mdl_base64: &str,
    remote_functions: Vec<RemoteFunction>,
    sql: &str,
) -> Result<String, CoreError> {
    let mdl_json_bytes = BASE64_STANDARD
        .decode(mdl_base64)
        .map_err(CoreError::from)?;
    let mdl_json = String::from_utf8(mdl_json_bytes).map_err(CoreError::from)?;
    let manifest = serde_json::from_str::<Manifest>(&mdl_json)?;
    let remote_functions: Vec<mdl::function::RemoteFunction> = remote_functions
        .into_iter()
        .map(|f| f.into())
        .collect::<Vec<_>>();

    let Ok(analyzed_mdl) = AnalyzedWrenMDL::analyze(manifest) else {
        return Err(CoreError::new("Failed to analyze manifest"));
    };
    match mdl::transform_sql(Arc::new(analyzed_mdl), &remote_functions, sql) {
        Ok(transformed_sql) => Ok(transformed_sql),
        Err(e) => Err(CoreError::new(&e.to_string())),
    }
}

#[pyfunction]
fn read_remote_function_list(path: Option<&str>) -> Vec<RemoteFunction> {
    debug!(
        "Reading remote function list from {}",
        path.unwrap_or("path is not provided")
    );
    if let Some(path) = path {
        csv::Reader::from_path(path)
            .unwrap()
            .into_deserialize::<RemoteFunction>()
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
    } else {
        vec![]
    }
}

#[pymodule]
#[pyo3(name = "wren_core")]
fn wren_core_wrapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    env_logger::init();
    m.add_function(wrap_pyfunction!(transform_sql, m)?)?;
    m.add_function(wrap_pyfunction!(read_remote_function_list, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use serde_json::Value;

    use crate::{read_remote_function_list, transform_sql};

    #[test]
    fn test_transform_sql() {
        let data = r#"
        {
            "catalog": "my_catalog",
            "schema": "my_schema",
            "models": [
                {
                    "name": "customer",
                    "tableReference": {
                        "schema": "main",
                        "table": "customer"
                    },
                    "columns": [
                        {"name": "c_custkey", "type": "integer"},
                        {"name": "c_name", "type": "varchar"}
                    ],
                    "primaryKey": "c_custkey"
                }
            ]
        }"#;
        let v: Value = serde_json::from_str(data).unwrap();
        let mdl_base64: String = BASE64_STANDARD.encode(v.to_string().as_bytes());
        let transformed_sql = transform_sql(
            &mdl_base64,
            vec![],
            "SELECT * FROM my_catalog.my_schema.customer",
        )
        .unwrap();
        assert_eq!(
            transformed_sql,
            "SELECT customer.c_custkey, customer.c_name FROM \
            (SELECT main.customer.c_custkey AS c_custkey, main.customer.c_name AS c_name FROM main.customer) AS customer"
        );
    }

    #[test]
    fn test_read_remote_function_list() {
        let path = "tests/functions.csv";
        let remote_functions = read_remote_function_list(Some(path));
        assert_eq!(remote_functions.len(), 3);

        let remote_function = read_remote_function_list(None);
        assert_eq!(remote_function.len(), 0);
    }
}
