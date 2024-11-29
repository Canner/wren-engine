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

use crate::errors::CoreError;
use crate::manifest::PyManifest;
use crate::remote_functions::PyRemoteFunction;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use log::debug;
use pyo3::{pyclass, pymethods, PyErr, PyResult};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use wren_core::logical_plan::utils::map_data_type;
use wren_core::mdl::context::create_ctx_with_mdl;
use wren_core::mdl::function::{
    ByPassAggregateUDF, ByPassScalarUDF, ByPassWindowFunction, FunctionType,
    RemoteFunction,
};
use wren_core::mdl::manifest::Manifest;
use wren_core::{mdl, AggregateUDF, AnalyzedWrenMDL, ScalarUDF, WindowUDF};

/// The Python wrapper for the Wren Core session context.
#[pyclass(name = "SessionContext")]
#[derive(Clone)]
pub struct PySessionContext {
    ctx: wren_core::SessionContext,
    mdl: Arc<AnalyzedWrenMDL>,
    remote_functions: Vec<RemoteFunction>,
}

impl Hash for PySessionContext {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mdl.hash(state);
        self.remote_functions.hash(state);
    }
}

impl Default for PySessionContext {
    fn default() -> Self {
        Self {
            ctx: wren_core::SessionContext::new(),
            mdl: Arc::new(AnalyzedWrenMDL::default()),
            remote_functions: vec![],
        }
    }
}

#[pymethods]
impl PySessionContext {
    /// Create a new session context.
    ///
    /// if `mdl_base64` is provided, the session context will be created with the given MDL. Otherwise, an empty MDL will be created.
    /// if `remote_functions_path` is provided, the session context will be created with the remote functions defined in the CSV file.
    #[new]
    #[pyo3(signature = (mdl_base64=None, remote_functions_path=None))]
    pub fn new(
        mdl_base64: Option<&str>,
        remote_functions_path: Option<&str>,
    ) -> PyResult<Self> {
        let remote_functions = Self::read_remote_function_list(remote_functions_path)
            .map_err(CoreError::from)?;
        let remote_functions: Vec<RemoteFunction> = remote_functions
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<_>>();

        let ctx = wren_core::SessionContext::new();

        let Some(mdl_base64) = mdl_base64 else {
            return Ok(Self {
                ctx,
                mdl: Arc::new(AnalyzedWrenMDL::default()),
                remote_functions,
            });
        };

        let mdl_json_bytes = BASE64_STANDARD
            .decode(mdl_base64)
            .map_err(CoreError::from)?;
        let mdl_json = String::from_utf8(mdl_json_bytes).map_err(CoreError::from)?;
        let manifest =
            serde_json::from_str::<Manifest>(&mdl_json).map_err(CoreError::from)?;

        let Ok(analyzed_mdl) = AnalyzedWrenMDL::analyze(manifest) else {
            return Err(CoreError::new("Failed to analyze manifest").into());
        };

        let analyzed_mdl = Arc::new(analyzed_mdl);

        let runtime = tokio::runtime::Runtime::new().map_err(CoreError::from)?;
        let ctx = runtime
            .block_on(create_ctx_with_mdl(&ctx, Arc::clone(&analyzed_mdl), false))
            .map_err(CoreError::from)?;

        remote_functions.iter().try_for_each(|remote_function| {
            debug!("Registering remote function: {:?}", remote_function);
            Self::register_remote_function(&ctx, remote_function)?;
            Ok::<(), CoreError>(())
        })?;

        Ok(Self {
            ctx,
            mdl: analyzed_mdl,
            remote_functions,
        })
    }

    /// Transform the given Wren SQL to the equivalent Planned SQL.
    pub fn transform_sql(&self, sql: &str) -> PyResult<String> {
        mdl::transform_sql(Arc::clone(&self.mdl), &self.remote_functions, sql)
            .map_err(|e| PyErr::from(CoreError::from(e)))
    }

    /// Get the available functions in the session context.
    pub fn get_available_functions(&self) -> PyResult<Vec<PyRemoteFunction>> {
        let mut builder = self
            .remote_functions
            .iter()
            .map(|f| (f.name.clone(), f.clone().into()))
            .collect::<HashMap<String, PyRemoteFunction>>();
        self.ctx
            .state()
            .scalar_functions()
            .iter()
            .for_each(|(name, _func)| {
                match builder.entry(name.clone()) {
                    Entry::Occupied(_) => {}
                    Entry::Vacant(entry) => {
                        entry.insert(PyRemoteFunction {
                            function_type: "scalar".to_string(),
                            name: name.clone(),
                            // TODO: get function return type from SessionState
                            return_type: None,
                            param_names: None,
                            param_types: None,
                            description: None,
                        });
                    }
                }
            });
        self.ctx
            .state()
            .aggregate_functions()
            .iter()
            .for_each(|(name, _func)| {
                match builder.entry(name.clone()) {
                    Entry::Occupied(_) => {}
                    Entry::Vacant(entry) => {
                        entry.insert(PyRemoteFunction {
                            function_type: "aggregate".to_string(),
                            name: name.clone(),
                            // TODO: get function return type from SessionState
                            return_type: None,
                            param_names: None,
                            param_types: None,
                            description: None,
                        });
                    }
                }
            });
        self.ctx
            .state()
            .window_functions()
            .iter()
            .for_each(|(name, _func)| {
                match builder.entry(name.clone()) {
                    Entry::Occupied(_) => {}
                    Entry::Vacant(entry) => {
                        entry.insert(PyRemoteFunction {
                            function_type: "window".to_string(),
                            name: name.clone(),
                            // TODO: get function return type from SessionState
                            return_type: None,
                            param_names: None,
                            param_types: None,
                            description: None,
                        });
                    }
                }
            });
        Ok(builder.values().cloned().collect())
    }

    /// parse the given SQL and return the list of used table name.
    pub fn resolve_used_table_names(&self, sql: &str) -> Result<Vec<String>, CoreError> {
        let mdl = self.mdl.wren_mdl();
        self.ctx
            .state()
            .sql_to_statement(sql, "generic")
            .map_err(CoreError::from)
            .and_then(|stmt| {
                self.ctx
                    .state()
                    .resolve_table_references(&stmt)
                    .map_err(CoreError::from)
            })
            .map(|tables| {
                tables
                    .iter()
                    .filter(|t| {
                        t.catalog().unwrap_or_default() == mdl.catalog()
                            && t.schema().unwrap_or_default() == mdl.schema()
                    })
                    .map(|t| t.table().to_string())
                    .collect()
            })
    }

    /// Given a used dataset list, extract manifest by removing unused datasets.
    /// If a model is related to another dataset, both datasets will be kept.
    /// The relationship between of them will be kept as well.
    /// A dataset could be model, view.
    pub fn extract_manifest(&self, used_datasets: Vec<String>) -> PyResult<PyManifest> {
        Ok(extractor::extract_manifest(self, &used_datasets))
    }
}

impl PySessionContext {
    fn register_remote_function(
        ctx: &wren_core::SessionContext,
        remote_function: &RemoteFunction,
    ) -> PyResult<()> {
        match &remote_function.function_type {
            FunctionType::Scalar => {
                ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
                    &remote_function.name,
                    map_data_type(&remote_function.return_type)
                        .map_err(CoreError::from)?,
                )))
            }
            FunctionType::Aggregate => {
                ctx.register_udaf(AggregateUDF::new_from_impl(ByPassAggregateUDF::new(
                    &remote_function.name,
                    map_data_type(&remote_function.return_type)
                        .map_err(CoreError::from)?,
                )))
            }
            FunctionType::Window => {
                ctx.register_udwf(WindowUDF::new_from_impl(ByPassWindowFunction::new(
                    &remote_function.name,
                    map_data_type(&remote_function.return_type)
                        .map_err(CoreError::from)?,
                )))
            }
        }
        Ok(())
    }

    fn read_remote_function_list(path: Option<&str>) -> PyResult<Vec<PyRemoteFunction>> {
        debug!(
            "Reading remote function list from {}",
            path.unwrap_or("path is not provided")
        );
        if let Some(path) = path {
            Ok(csv::Reader::from_path(path)
                .map_err(CoreError::from)?
                .into_deserialize::<PyRemoteFunction>()
                .filter_map(Result::ok)
                .collect::<Vec<_>>())
        } else {
            Ok(vec![])
        }
    }
}

mod extractor {
    use crate::context::PySessionContext;
    use crate::manifest::PyManifest;
    use std::collections::HashSet;
    use std::sync::Arc;
    use wren_core::mdl::manifest::{Model, Relationship, View};
    use wren_core::mdl::WrenMDL;

    pub fn extract_manifest(
        ctx: &PySessionContext,
        used_datasets: &[String],
    ) -> PyManifest {
        let mdl = Arc::clone(&ctx.mdl).wren_mdl();
        let used_models = extract_models(&mdl, used_datasets);
        let (used_views, models_of_views) = extract_views(&ctx, &mdl, used_datasets);
        let used_relationships = extract_relationships(&mdl, used_datasets);
        PyManifest {
            catalog: mdl.catalog().to_string(),
            schema: mdl.schema().to_string(),
            models: [used_models, models_of_views].concat(),
            relationships: used_relationships,
            metrics: mdl.metrics().to_vec(),
            views: used_views,
        }
    }

    fn extract_models(mdl: &Arc<WrenMDL>, used_datasets: &[String]) -> Vec<Arc<Model>> {
        let mut used_set: HashSet<String> = used_datasets.iter().cloned().collect();
        let mut stack: Vec<String> = used_datasets.to_vec();
        while let Some(dataset_name) = stack.pop() {
            if let Some(model) = mdl.get_model(&dataset_name) {
                model
                    .columns
                    .iter()
                    .filter_map(|col| {
                        col.relationship
                            .as_ref()
                            .and_then(|rel_name| mdl.get_relationship(rel_name))
                    })
                    .flat_map(|rel| rel.models.clone())
                    .filter(|related| used_set.insert(related.clone()))
                    .for_each(|related| stack.push(related));
            }
        }
        mdl.models()
            .iter()
            .filter(|model| used_set.contains(model.name()))
            .cloned()
            .collect()
    }

    fn extract_views(
        ctx: &PySessionContext,
        mdl: &Arc<WrenMDL>,
        used_datasets: &[String],
    ) -> (Vec<Arc<View>>, Vec<Arc<Model>>) {
        let used_set: HashSet<&str> = used_datasets.iter().map(String::as_str).collect();
        let stack: Vec<&str> = used_datasets.iter().map(String::as_str).collect();
        let models = stack
            .iter()
            .filter_map(|&dataset_name| {
                mdl.get_view(dataset_name).and_then(|view| {
                    ctx.resolve_used_table_names(&view.statement)
                        .ok()
                        .map(|used_tables| extract_models(mdl, &used_tables))
                })
            })
            .flatten()
            .collect::<Vec<_>>();
        let views = mdl
            .views()
            .iter()
            .filter(|view| used_set.contains(view.name()))
            .cloned()
            .collect();

        (views, models)
    }

    fn extract_relationships(
        mdl: &Arc<WrenMDL>,
        used_datasets: &[String],
    ) -> Vec<Arc<Relationship>> {
        let mut used_set: HashSet<String> = used_datasets.iter().cloned().collect();
        let mut stack: Vec<String> = used_datasets.to_vec();
        while let Some(dataset_name) = stack.pop() {
            if let Some(relationship) = mdl.get_relationship(&dataset_name) {
                for model in &relationship.models {
                    if used_set.insert(model.clone()) {
                        stack.push(model.clone());
                    }
                }
            }
        }
        mdl.relationships()
            .iter()
            .filter(|rel| rel.models.iter().any(|model| used_set.contains(model)))
            .cloned()
            .collect()
    }
}
