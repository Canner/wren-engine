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
use crate::fuzz_case_generator::FuzzCaseGenerator;
use crate::manifest::to_manifest;
use crate::remote_functions::PyRemoteFunction;
use log::debug;
use pyo3::{pyclass, pymethods, PyErr, PyResult};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::Arc;
use std::vec;
use tokio::runtime::Runtime;
use wren_core::datafusion::arrow::array::AsArray;
use wren_core::datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use wren_core::datafusion::prelude::{SessionConfig, SessionContext};
use wren_core::datafusion::sql::sqlparser::ast::{
    visit_statements_mut, Expr, Statement, Value,
};
use wren_core::datafusion::sql::sqlparser::dialect::GenericDialect;
use wren_core::mdl::context::create_ctx_with_mdl;
use wren_core::mdl::function::{
    ByPassAggregateUDF, ByPassScalarUDF, ByPassWindowFunction, FunctionType,
    RemoteFunction,
};
use wren_core::{mdl, AnalyzedWrenMDL};

/// The Python wrapper for the Wren Core session context.
#[pyclass(name = "SessionContext")]
#[derive(Clone)]
pub struct PySessionContext {
    ctx: SessionContext,
    mdl: Arc<AnalyzedWrenMDL>,
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
            ctx: SessionContext::new(),
            mdl: Arc::new(AnalyzedWrenMDL::default()),
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

        let config = SessionConfig::default().with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);
        let runtime = Runtime::new().map_err(CoreError::from)?;

        let registered_functions = runtime
            .block_on(Self::get_regietered_functions(&ctx))
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
                    Self::register_remote_function(&ctx, remote_function)?;
                }
                Ok::<(), CoreError>(())
            })?;

        let Some(mdl_base64) = mdl_base64 else {
            return Ok(Self {
                ctx,
                mdl: Arc::new(AnalyzedWrenMDL::default()),
                runtime: Arc::new(runtime),
            });
        };

        let manifest = to_manifest(mdl_base64)?;

        let Ok(analyzed_mdl) = AnalyzedWrenMDL::analyze(manifest) else {
            return Err(CoreError::new("Failed to analyze manifest").into());
        };

        let analyzed_mdl = Arc::new(analyzed_mdl);

        // the headers won't be used in the context. Provide an empty map.
        let ctx = runtime
            .block_on(create_ctx_with_mdl(
                &ctx,
                Arc::clone(&analyzed_mdl),
                Arc::new(HashMap::new()),
                false,
            ))
            .map_err(CoreError::from)?;

        Ok(Self {
            ctx,
            mdl: analyzed_mdl,
            runtime: Arc::new(runtime),
        })
    }

    /// Transform the given Wren SQL to the equivalent Planned SQL.
    #[pyo3(signature = (sql=None, properties=None))]
    pub fn transform_sql(
        &self,
        sql: Option<&str>,
        properties: Option<HashMap<String, Option<String>>>,
    ) -> PyResult<String> {
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
                properties.unwrap_or_default(),
                sql,
            ))
            .map_err(|e| PyErr::from(CoreError::from(e)))
    }

    /// Get the available functions in the session context.
    pub fn get_available_functions(&self) -> PyResult<Vec<PyRemoteFunction>> {
        let registered_functions: Vec<PyRemoteFunction> = self
            .runtime
            .block_on(Self::get_regietered_functions(&self.ctx))
            .map_err(CoreError::from)?
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<_>>();
        Ok(registered_functions)
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
            wren_core::datafusion::sql::sqlparser::parser::Parser::parse_sql(
                &GenericDialect {},
                sql,
            )
            .map_err(CoreError::from)?;
        if statements.len() != 1 {
            return Err(CoreError::new("Only one statement is allowed").into());
        }
        visit_statements_mut(&mut statements, |stmt| {
            if let Statement::Query(q) = stmt {
                if let Some(limit) = &q.limit {
                    if let Expr::Value(Value::Number(n, is)) = limit {
                        if n.parse::<usize>().unwrap() > pushdown {
                            q.limit = Some(Expr::Value(Value::Number(
                                pushdown.to_string(),
                                *is,
                            )));
                        }
                    }
                } else {
                    q.limit =
                        Some(Expr::Value(Value::Number(pushdown.to_string(), false)));
                }
            }
            ControlFlow::<()>::Continue(())
        });
        Ok(statements[0].to_string())
    }

    pub fn generate_scalar_function_fuzz_case(&self) -> PyResult<Vec<String>> {
        let generator = FuzzCaseGenerator::new();
        let state = self.ctx.state();
        let functions = state
            .scalar_functions()
            .iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        let result = functions
            .chunks(50)
            .map(|chunk| {
                let fs = chunk.iter().map(|f| Arc::clone(f)).collect::<Vec<_>>();
                generator.generate_scalar(&fs).map_err(CoreError::from)
            })
            .collect::<Result<_, CoreError>>()?;
        Ok(result)
    }
}

impl PySessionContext {
    fn register_remote_function(
        ctx: &SessionContext,
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
    async fn get_regietered_functions(
        ctx: &SessionContext,
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
