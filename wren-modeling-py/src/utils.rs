use crate::TokioRuntime;
use pyo3::prelude::PyAnyMethods;
use pyo3::{Bound, Py, PyRef, Python};
use std::future::Future;
use tokio::runtime::Runtime;

/// Utility to get the Tokio Runtime from Python
pub(crate) fn get_tokio_runtime(py: Python) -> PyRef<TokioRuntime> {
    let rt = TokioRuntime(Runtime::new().unwrap());
    let obj: Bound<'_, TokioRuntime> = Py::new(py, rt).unwrap().into_bound(py);
    obj.extract().unwrap()
}

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F>(py: Python, f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime: &Runtime = &get_tokio_runtime(py).0;
    py.allow_threads(|| runtime.block_on(f))
}
