// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use pyo3::prelude::PyDictMethods;
use pyo3::types::PyDict;
use pyo3::{pyclass, pymethods, PyObject, Python};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use wren_core::mdl::function::FunctionType;

#[pyclass(name = "RemoteFunction")]
#[derive(Serialize, Deserialize, Clone)]
pub struct PyRemoteFunction {
    pub function_type: String,
    pub name: String,
    pub return_type: Option<String>,
    /// It's a comma separated string of parameter names
    pub param_names: Option<String>,
    /// It's a comma separated string of parameter types
    pub param_types: Option<String>,
    pub description: Option<String>,
}

#[pymethods]
impl PyRemoteFunction {
    pub fn to_dict(&self, py: Python) -> PyObject {
        let dict = PyDict::new_bound(py);
        dict.set_item("function_type", self.function_type.clone())
            .unwrap();
        dict.set_item("name", self.name.clone()).unwrap();
        dict.set_item("return_type", self.return_type.clone())
            .unwrap();
        dict.set_item("param_names", self.param_names.clone())
            .unwrap();
        dict.set_item("param_types", self.param_types.clone())
            .unwrap();
        dict.set_item("description", self.description.clone())
            .unwrap();
        dict.into()
    }
}

impl From<wren_core::mdl::function::RemoteFunction> for PyRemoteFunction {
    fn from(remote_function: wren_core::mdl::function::RemoteFunction) -> Self {
        let param_names = remote_function.param_names.map(|names| {
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<String>>()
                .join(",")
        });
        let param_types = remote_function.param_types.map(|types| {
            types
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<String>>()
                .join(",")
        });
        Self {
            function_type: remote_function.function_type.to_string(),
            name: remote_function.name,
            return_type: Some(remote_function.return_type),
            param_names,
            param_types,
            description: remote_function.description,
        }
    }
}

impl From<PyRemoteFunction> for wren_core::mdl::function::RemoteFunction {
    fn from(
        remote_function: PyRemoteFunction,
    ) -> wren_core::mdl::function::RemoteFunction {
        let param_names = remote_function.param_names.map(|names| {
            names
                .split(",")
                .map(|name| name.to_string())
                .collect::<Vec<String>>()
        });
        let param_types = remote_function.param_types.map(|types| {
            types
                .split(",")
                .map(|t| t.to_string())
                .collect::<Vec<String>>()
        });
        wren_core::mdl::function::RemoteFunction {
            function_type: FunctionType::from_str(&remote_function.function_type)
                .unwrap(),
            name: remote_function.name,
            // TODO: Get the return type form DataFusion SessionState
            return_type: remote_function.return_type.unwrap_or("string".to_string()),
            param_names,
            param_types,
            description: remote_function.description,
        }
    }
}
