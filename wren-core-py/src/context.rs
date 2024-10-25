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

use std::hash::Hash;
use std::sync::Arc;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use log::debug;
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};
use wren_core::{mdl, AggregateUDF, AnalyzedWrenMDL, ScalarUDF, WindowUDF};
use wren_core::logical_plan::utils::map_data_type;
use wren_core::mdl::context::create_ctx_with_mdl;
use wren_core::mdl::function::{ByPassAggregateUDF, ByPassScalarUDF, ByPassWindowFunction, FunctionType, RemoteFunction};
use crate::remote_functions::PyRemoteFunction;
use crate::errors::CoreError;

#[pyclass]
#[derive(Serialize, Deserialize, Clone)]
pub struct PySessionContext {
    df_ctx: wren_core::SessionContext,
    mdl: Arc<AnalyzedWrenMDL>,
    remote_functions: Vec<RemoteFunction>,
}

impl Hash for PySessionContext {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mdl.hash(state);
        self.remote_functions.hash(state);
    }
}

#[pymethods]
impl PySessionContext {
    #[new]
    pub fn new(
        mdl_base64: &str,
        remote_functions_path: Option<&str>,
    ) -> Result<Self, CoreError> {
        let remote_functions = Self::read_remote_function_list(remote_functions_path);
        let remote_functions: Vec<RemoteFunction> = remote_functions
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<_>>();

        let mdl_json_bytes = BASE64_STANDARD
            .decode(mdl_base64)
            .map_err(CoreError::from)?;
        let mdl_json = String::from_utf8(mdl_json_bytes).map_err(CoreError::from)?;
        let manifest = serde_json::from_str::<mdl::manifest::Manifest>(&mdl_json)?;

        let Ok(analyzed_mdl) = AnalyzedWrenMDL::analyze(manifest) else {
            return Err(CoreError::new("Failed to analyze manifest"));
        };

        let analyzed_mdl = Arc::new(analyzed_mdl);

        let ctx = wren_core::SessionContext::new();
        let ctx = create_ctx_with_mdl(&ctx, Arc::clone(&analyzed_mdl), false);

        remote_functions.iter().for_each(|remote_function| {
            debug!("Registering remote function: {:?}", remote_function);
            Self::register_remote_function(&ctx, remote_function);
        });

        Ok(Self {
            df_ctx: ctx,
            mdl: analyzed_mdl,
            remote_functions,
        })
    }

    fn read_remote_function_list(path: Option<&str>) -> Vec<PyRemoteFunction> {
        debug!(
        "Reading remote function list from {}",
        path.unwrap_or("path is not provided")
    );
        if let Some(path) = path {
            csv::Reader::from_path(path)
                .unwrap()
                .into_deserialize::<PyRemoteFunction>()
                .filter_map(Result::ok)
                .collect::<Vec<_>>()
        } else {
            vec![]
        }
    }

    fn register_remote_function(
        ctx: &wren_core::SessionContext,
        remote_function: &RemoteFunction) {
        match &remote_function.function_type {
            FunctionType::Scalar => {
                ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
                    &remote_function.name,
                    map_data_type(&remote_function.return_type),
                )))
            }
            FunctionType::Aggregate => {
                ctx.register_udaf(AggregateUDF::new_from_impl(ByPassAggregateUDF::new(
                    &remote_function.name,
                    map_data_type(&remote_function.return_type),
                )))
            }
            FunctionType::Window => {
                ctx.register_udwf(WindowUDF::new_from_impl(ByPassWindowFunction::new(
                    &remote_function.name,
                    map_data_type(&remote_function.return_type),
                )))
            }
        }
    }

    pub fn transform_sql(&self, sql: &str) -> Result<String, CoreError> {
        mdl::transform_sql(Arc::clone(&self.mdl), &self.remote_functions, sql)
            .map_err(CoreError::from)
    }

    pub fn get_available_functions(&self) -> Vec<String> {

    }
}