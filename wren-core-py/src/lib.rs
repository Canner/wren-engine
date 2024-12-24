use pyo3::prelude::*;

use remote_functions::PyRemoteFunction;

pub mod context;
mod errors;
mod extractor;
mod manifest;
pub mod remote_functions;

#[pymodule]
#[pyo3(name = "wren_core")]
fn wren_core_wrapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    env_logger::init();
    m.add_class::<context::PySessionContext>()?;
    m.add_class::<PyRemoteFunction>()?;
    m.add_class::<manifest::Manifest>()?;
    m.add_class::<extractor::PyManifestExtractor>()?;
    m.add_function(wrap_pyfunction!(manifest::to_json_base64, m)?)?;
    Ok(())
}
