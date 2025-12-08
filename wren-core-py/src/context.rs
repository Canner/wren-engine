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
use crate::manifest::to_manifest;
use crate::remote_functions::PyRemoteFunction;
use log::debug;
use pyo3::types::{PyAnyMethods, PyFrozenSet, PyFrozenSetMethods, PyTuple};
use pyo3::Python;
use pyo3::{pyclass, pymethods, Py, PyAny, PyErr, PyResult};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::Arc;
use std::vec;
use tokio::runtime::Runtime;
use wren_core::array::{AsArray, GenericByteArray};
use wren_core::ast::{visit_statements_mut, Expr, Statement, Value, ValueWithSpan};
use wren_core::datatypes::GenericStringType;
use wren_core::dialect::GenericDialect;
use wren_core::mdl::context::apply_wren_on_ctx;
use wren_core::mdl::function::{
    ByPassAggregateUDF, ByPassScalarUDF, ByPassWindowFunction, FunctionType,
    RemoteFunction,
};
use wren_core::{
    mdl, AggregateUDF, AnalyzedWrenMDL, ScalarUDF, SessionConfig, WindowUDF,
};
use wren_core_base::mdl::DataSource;

/// The Python wrapper for the Wren Core session context.
#[pyclass(name = "SessionContext")]
#[derive(Clone)]
pub struct PySessionContext {
    ctx: wren_core::SessionContext,
    exec_ctx: wren_core::SessionContext,
    mdl: Arc<AnalyzedWrenMDL>,
    properties: Arc<HashMap<String, Option<String>>>,
    runtime: Arc<Runtime>,
}

impl Hash for PySessionContext {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mdl.hash(state);
    }
}

impl Default for PySessionContext {
    fn default() -> Self {
        Self {
            ctx: wren_core::SessionContext::new(),
            exec_ctx: wren_core::SessionContext::new(),
            mdl: Arc::new(AnalyzedWrenMDL::default()),
            properties: Arc::new(HashMap::new()),
            runtime: Arc::new(Runtime::new().unwrap()),
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
    #[pyo3(signature = (mdl_base64=None, remote_functions_path=None, properties=None, data_source=None))]
    pub fn new(
        mdl_base64: Option<&str>,
        remote_functions_path: Option<&str>,
        properties: Option<Py<PyAny>>,
        data_source: Option<&str>,
    ) -> PyResult<Self> {
        let runtime = Runtime::new().map_err(CoreError::from)?;

        let Some(mdl_base64) = mdl_base64 else {
            let data_source = data_source
                .map(|ds| DataSource::from_str(ds).map_err(CoreError::from))
                .transpose()?;
            let config = SessionConfig::default().with_information_schema(true);
            let ctx = wren_core::mdl::create_wren_ctx(Some(config), data_source.as_ref());
            Self::register_function_by_data_source(
                data_source.as_ref(),
                remote_functions_path,
                &runtime,
                &ctx,
            )?;
            return Ok(Self {
                ctx: ctx.clone(),
                exec_ctx: ctx,
                mdl: Arc::new(AnalyzedWrenMDL::default()),
                properties: Arc::new(HashMap::new()),
                runtime: Arc::new(runtime),
            });
        };

        let manifest = to_manifest(mdl_base64)?;

        // If the manifest has a data source, use it.
        // Otherwise, if the data_source parameter is provided, use it.
        // Otherwise, use None.
        let data_source = if let Some(ds) = &manifest.data_source {
            Some(*ds)
        } else if let Some(ds_str) = data_source {
            Some(DataSource::from_str(ds_str).map_err(CoreError::from)?)
        } else {
            None
        };

        let config = SessionConfig::default().with_information_schema(true);
        let ctx = wren_core::mdl::create_wren_ctx(Some(config), data_source.as_ref());

        Self::register_function_by_data_source(
            data_source.as_ref(),
            remote_functions_path,
            &runtime,
            &ctx,
        )?;

        Python::attach(|py: Python<'_>| {
            let properties_map = if let Some(obj) = properties {
                let obj = obj.as_ref();
                if obj.is_none(py) {
                    HashMap::new()
                } else {
                    let frozenset = obj.downcast_bound::<PyFrozenSet>(py)?;
                    let mut map = HashMap::new();
                    for item in frozenset.iter() {
                        match item.as_any().clone().downcast_into::<PyTuple>() {
                            Ok(tuple) => {
                                if tuple.len()? != 2 {
                                    return Err(CoreError::new(
                                        "Properties must be a tuple of (key, value)",
                                    )
                                    .into());
                                }
                                let key = tuple.get_item(0)?.to_string();
                                let value = tuple.get_item(1)?.to_string();
                                map.insert(key, Some(value));
                            }
                            Err(_) => {
                                return Err(CoreError::new(
                                    "Properties must be a tuple of (key, value)",
                                )
                                .into());
                            }
                        }
                    }
                    map
                }
            } else {
                HashMap::new()
            };
            let properties_ref = Arc::new(properties_map);
            match AnalyzedWrenMDL::analyze(
                manifest,
                Arc::clone(&properties_ref),
                mdl::context::Mode::Unparse,
            ) {
                Ok(analyzed_mdl) => {
                    let analyzed_mdl = Arc::new(analyzed_mdl);
                    let unparser_ctx = runtime
                        .block_on(apply_wren_on_ctx(
                            &ctx,
                            Arc::clone(&analyzed_mdl),
                            Arc::clone(&properties_ref),
                            mdl::context::Mode::Unparse,
                        ))
                        .map_err(CoreError::from)?;

                    let exec_ctx = runtime
                        .block_on(apply_wren_on_ctx(
                            &ctx,
                            Arc::clone(&analyzed_mdl),
                            Arc::clone(&properties_ref),
                            mdl::context::Mode::LocalRuntime,
                        ))
                        .map_err(CoreError::from)?;

                    Ok(Self {
                        ctx: unparser_ctx,
                        exec_ctx,
                        mdl: analyzed_mdl,
                        runtime: Arc::new(runtime),
                        properties: properties_ref,
                    })
                }
                Err(e) => Err(CoreError::new(
                    format!("Failed to analyze MDL: {}", e).as_str(),
                )
                .into()),
            }
        })
    }

    /// Transform the given Wren SQL to the equivalent Planned SQL.
    #[pyo3(signature = (sql=None))]
    pub fn transform_sql(&self, sql: Option<&str>) -> PyResult<String> {
        env_logger::try_init().ok();
        let Some(sql) = sql else {
            return Err(CoreError::new("SQL is required").into());
        };
        self.runtime
            .block_on(mdl::transform_sql_with_ctx(
                &self.ctx,
                Arc::clone(&self.mdl),
                // the ctx has been initialized when PySessionContext is created
                // so we can pass the empty array here
                &[],
                Arc::clone(&self.properties),
                sql,
            ))
            .map_err(|e| PyErr::from(CoreError::from(e)))
    }

    /// Get the available functions in the session context.
    pub fn get_available_functions(&self) -> PyResult<Vec<PyRemoteFunction>> {
        let registered_functions: Vec<PyRemoteFunction> = self
            .runtime
            .block_on(Self::get_registered_functions(&self.exec_ctx))
            .map_err(CoreError::from)?
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<_>>();
        Ok(registered_functions)
    }

    pub fn get_available_function(
        &self,
        function_name: &str,
    ) -> PyResult<Vec<PyRemoteFunction>> {
        let functions = self
            .runtime
            .block_on(Self::get_registered_function(function_name, &self.exec_ctx))
            .map_err(CoreError::from)?
            .into_iter()
            .map(|f| PyRemoteFunction::from(f))
            .collect::<Vec<_>>();
        Ok(functions)
    }

    /// Push down the limit to the given SQL.
    /// If the limit is None, the SQL will be returned as is.
    /// If the limit is greater than the pushdown limit, the limit will be replaced with the pushdown limit.
    /// Otherwise, the limit will be kept as is.
    #[pyo3(signature = (sql, limit=None))]
    pub fn pushdown_limit(&self, sql: &str, limit: Option<usize>) -> PyResult<String> {
        if limit.is_none() {
            return Ok(sql.to_string());
        }
        let pushdown = limit.unwrap();
        let mut statements =
            wren_core::parser::Parser::parse_sql(&GenericDialect {}, sql)
                .map_err(CoreError::from)?;
        if statements.len() != 1 {
            return Err(CoreError::new("Only one statement is allowed").into());
        }
        let _ = visit_statements_mut(&mut statements, |stmt| {
            if let Statement::Query(q) = stmt {
                if let Some(limit) = &q.limit {
                    if let Expr::Value(ValueWithSpan {
                        value: Value::Number(n, is),
                        ..
                    }) = limit
                    {
                        if let Ok(curr) = n.parse::<usize>() {
                            if curr > pushdown {
                                q.limit = Some(Expr::Value(
                                    Value::Number(pushdown.to_string(), *is).into(),
                                ));
                            }
                        }
                    }
                } else {
                    q.limit = Some(Expr::Value(
                        Value::Number(pushdown.to_string(), false).into(),
                    ));
                }
            }
            ControlFlow::<()>::Continue(())
        });
        Ok(statements[0].to_string())
    }
}

impl PySessionContext {
    fn register_remote_function(
        ctx: &wren_core::SessionContext,
        remote_function: RemoteFunction,
    ) -> PyResult<()> {
        match &remote_function.function_type {
            FunctionType::Scalar => {
                let func: ByPassScalarUDF = remote_function.into();
                ctx.register_udf(ScalarUDF::new_from_impl(func))
            }
            FunctionType::Aggregate => {
                let func: ByPassAggregateUDF = remote_function.into();
                ctx.register_udaf(AggregateUDF::new_from_impl(func))
            }
            FunctionType::Window => {
                let func: ByPassWindowFunction = remote_function.into();
                ctx.register_udwf(WindowUDF::new_from_impl(func))
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

    /// Get the registered functions in the session context.
    /// Only return `name`, `function_type`, and `description`.
    /// The `name` is the name of the function.
    /// The `function_type` is the type of the function. (e.g. scalar, aggregate, window)
    /// The `description` is the description of the function.
    async fn get_registered_functions(
        ctx: &wren_core::SessionContext,
    ) -> PyResult<Vec<RemoteFunctionDto>> {
        let sql = r#"
            SELECT DISTINCT
                r.routine_name as name,
                r.function_type,
                r.description
            FROM
                information_schema.routines r
        "#;
        let batches = ctx
            .sql(sql)
            .await
            .map_err(CoreError::from)?
            .collect()
            .await
            .map_err(CoreError::from)?;
        let mut functions = vec![];

        for batch in batches {
            let name_array = batch.column(0).as_string::<i32>();
            let function_type_array = batch.column(1).as_string::<i32>();
            let description_array = batch.column(2).as_string::<i32>();

            for row in 0..batch.num_rows() {
                let name = name_array.value(row).to_string();
                let description = description_array.value(row).to_string();
                let function_type = function_type_array.value(row).to_string();

                functions.push(RemoteFunctionDto {
                    name,
                    description: Some(description),
                    function_type: FunctionType::from_str(&function_type).unwrap(),
                });
            }
        }
        Ok(functions)
    }

    fn register_function_by_data_source(
        data_source: Option<&DataSource>,
        remote_functions_path: Option<&str>,
        runtime: &Runtime,
        ctx: &wren_core::SessionContext,
    ) -> PyResult<()> {
        match data_source {
            Some(DataSource::BigQuery) => {}
            _ => {
                let remote_functions =
                    Self::read_remote_function_list(remote_functions_path)
                        .map_err(CoreError::from)?;
                let remote_functions: Vec<RemoteFunction> = remote_functions
                    .into_iter()
                    .map(|f| f.into())
                    .collect::<Vec<_>>();

                let registered_functions = runtime
                    .block_on(Self::get_registered_functions(ctx))
                    .map(|functions| {
                        functions
                            .into_iter()
                            .map(|f| f.name)
                            .collect::<std::collections::HashSet<String>>()
                    })
                    .map_err(CoreError::from)?;

                remote_functions
                    .into_iter()
                    .try_for_each(|remote_function| {
                        debug!("Registering remote function: {:?}", remote_function);
                        // TODO: check not only the name but also the return type and the parameter types
                        if !registered_functions.contains(&remote_function.name) {
                            Self::register_remote_function(ctx, remote_function)?;
                        }
                        Ok::<(), CoreError>(())
                    })?;
            }
        }
        Ok(())
    }

    async fn get_registered_function(
        function_name: &str,
        ctx: &wren_core::SessionContext,
    ) -> PyResult<Vec<RemoteFunction>> {
        let sql = format!(
            r#"
            WITH inputs AS (
                SELECT
                    r.specific_name,
                    r.data_type as return_type,
                    pi.rid,
                    array_agg(pi.parameter_name order by pi.ordinal_position) as param_names,
                    array_agg(pi.data_type order by pi.ordinal_position) as param_types
                FROM
                    information_schema.routines r
                JOIN
                    information_schema.parameters pi ON r.specific_name = pi.specific_name AND pi.parameter_mode = 'IN'
                GROUP BY 1, 2, 3
            )
            SELECT
                r.routine_name as name,
                i.param_names,
                i.param_types,
                r.data_type as return_type,
                r.function_type,
                r.description
            FROM
                information_schema.routines r
            LEFT JOIN
                inputs i ON r.specific_name = i.specific_name
            WHERE
                r.routine_name = '{}'
        "#,
            function_name
        );
        let batches = ctx
            .sql(&sql)
            .await
            .map_err(CoreError::from)?
            .collect()
            .await
            .map_err(CoreError::from)?;
        let mut functions = vec![];

        for batch in batches {
            let name_array = batch.column(0).as_string::<i32>();
            let param_names_array = batch.column(1).as_list::<i32>();
            let param_types_array = batch.column(2).as_list::<i32>();
            let return_type_array = batch.column(3).as_string::<i32>();
            let function_type_array = batch.column(4).as_string::<i32>();
            let description_array = batch.column(5).as_string::<i32>();

            for row in 0..batch.num_rows() {
                let name = name_array.value(row).to_string();
                let param_names =
                    Self::to_string_vec(param_names_array.value(row).as_string::<i32>());
                let param_types =
                    Self::to_string_vec(param_types_array.value(row).as_string::<i32>());
                let return_type = return_type_array.value(row).to_string();
                let description = description_array.value(row).to_string();
                let function_type = function_type_array.value(row).to_string();

                functions.push(RemoteFunction {
                    name,
                    param_names: Some(param_names),
                    param_types: Some(param_types),
                    return_type,
                    description: Some(description),
                    function_type: FunctionType::from_str(&function_type).unwrap(),
                });
            }
        }
        Ok(functions)
    }

    fn to_string_vec(
        array: &GenericByteArray<GenericStringType<i32>>,
    ) -> Vec<Option<String>> {
        array
            .iter()
            .map(|s| s.map(|s| s.to_string()))
            .collect::<Vec<Option<String>>>()
    }
}

struct RemoteFunctionDto {
    name: String,
    function_type: FunctionType,
    description: Option<String>,
}

impl From<RemoteFunctionDto> for PyRemoteFunction {
    fn from(remote_function: RemoteFunctionDto) -> Self {
        Self {
            function_type: remote_function.function_type.to_string(),
            name: remote_function.name,
            return_type: None,
            param_names: None,
            param_types: None,
            description: remote_function.description,
        }
    }
}
