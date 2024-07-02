use std::sync::Arc;

use base64::prelude::*;
use pyo3::prelude::*;

use wren_core::mdl;
use wren_core::mdl::manifest::Manifest;
use wren_core::mdl::AnalyzedWrenMDL;
use wren_core::SessionContext;

use crate::errors::CoreError;
use crate::utils::wait_for_future;

mod errors;
mod utils;

// Used to define Tokio Runtime as a Python module attribute
#[pyclass]
pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[pyfunction]
fn transform_sql(mdl_base64: &str, sql: &str, py: Python) -> Result<String, CoreError> {
    let mdl_json_bytes = BASE64_STANDARD
        .decode(mdl_base64)
        .map_err(CoreError::from)?;
    let mdl_json = String::from_utf8(mdl_json_bytes).map_err(CoreError::from)?;
    let manifest = serde_json::from_str::<Manifest>(&mdl_json)?;

    let Ok(analyzed_mdl) = AnalyzedWrenMDL::analyze(manifest) else {
        return Err(CoreError::new("Failed to analyze manifest"));
    };
    wait_for_future(py, mdl::transform_sql(&SessionContext::new(), Arc::new(analyzed_mdl), sql)).map_err(CoreError::from)
}

#[pymodule]
#[pyo3(name = "wren_core")]
fn wren_core_wrapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(transform_sql, m)?)?;
    Ok(())
}
