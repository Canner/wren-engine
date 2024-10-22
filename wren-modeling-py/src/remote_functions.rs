use pyo3::pyclass;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use wren_core::mdl::function::FunctionType;

#[pyclass]
#[derive(Serialize, Deserialize, Clone)]
pub struct RemoteFunction {
    pub function_type: String,
    pub name: String,
    pub return_type: String,
    pub description: Option<String>,
}

impl From<wren_core::mdl::function::RemoteFunction> for RemoteFunction {
    fn from(remote_function: wren_core::mdl::function::RemoteFunction) -> Self {
        Self {
            function_type: remote_function.function_type.to_string(),
            name: remote_function.name,
            return_type: remote_function.return_type,
            description: remote_function.description,
        }
    }
}

impl From<RemoteFunction> for wren_core::mdl::function::RemoteFunction {
    fn from(remote_function: RemoteFunction) -> wren_core::mdl::function::RemoteFunction {
        wren_core::mdl::function::RemoteFunction {
            function_type: FunctionType::from_str(&remote_function.function_type)
                .unwrap(),
            name: remote_function.name,
            return_type: remote_function.return_type,
            description: remote_function.description,
        }
    }
}
