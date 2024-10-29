use pyo3::prelude::*;

use remote_functions::PyRemoteFunction;

pub mod context;
mod errors;
pub mod remote_functions;

#[pymodule]
#[pyo3(name = "wren_core")]
fn wren_core_wrapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    env_logger::init();
    m.add_class::<context::PySessionContext>()?;
    m.add_class::<PyRemoteFunction>()?;
    Ok(())
}
