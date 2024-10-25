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
