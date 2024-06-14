use pyo3::{pyclass, pymethods, PyResult};

use wren_core::mdl::builder::ManifestBuilder;
use wren_core::mdl::manifest::Manifest;

#[pyclass(module = "modeling_core")]
#[pyo3(name = "Manifest")]
pub struct ManifestWrapper(Manifest);

impl From<Manifest> for ManifestWrapper {
    fn from(inner: Manifest) -> Self {
        ManifestWrapper(inner)
    }
}

impl From<ManifestWrapper> for Manifest {
    fn from(inner: ManifestWrapper) -> Self {
        inner.0
    }
}

#[pyclass(module = "modeling_core")]
#[pyo3(name = "ManifestBuilder")]
pub struct ManifestBuilderWrapper(ManifestBuilder);

impl From<ManifestBuilder> for ManifestBuilderWrapper {
    fn from(inner: ManifestBuilder) -> Self {
        ManifestBuilderWrapper(inner)
    }
}

impl From<ManifestBuilderWrapper> for ManifestBuilder {
    fn from(inner: ManifestBuilderWrapper) -> Self {
        inner.0
    }
}

#[pymethods]
impl ManifestBuilderWrapper {
    pub fn build(&self) -> PyResult<ManifestWrapper> {
        Ok(ManifestWrapper::from(self.into()))
    }
}
