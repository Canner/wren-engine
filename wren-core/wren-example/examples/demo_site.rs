/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::Visit;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::{fs, io};
use wren_core::logical_plan::utils::try_map_data_type;
use wren_core::mdl::function::ByPassScalarUDF;
use wren_core::mdl::manifest::Manifest;
use wren_core::mdl::{transform_sql_with_ctx, AnalyzedWrenMDL};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let mdl_json = "/Users/jax/git/wren-engine/ibis-server/etc.local/local_mdl.json";
    let json_string = fs::read_to_string(mdl_json).unwrap();
    let manifest: Manifest = serde_json::from_str(&json_string).unwrap();
    let mdl = Arc::new(AnalyzedWrenMDL::analyze(manifest)?);
    //
    // let sql = "SELECT orders_key FROM (select * from orders limit 1000) as t";
    // let mut statements = wren_core::parser::Parser::parse_sql(&GenericDialect {}, sql)?;
    // let pushdown_limit = 100;
    //
    // visit_statements_mut(&mut statements, |stmt| {
    //     if let Statement::Query(q) = stmt {
    //         if let Some(limit) = &q.limit {
    //             if let Expr::Value(Value::Number(n, is)) = limit {
    //                 if n.parse::<usize>().unwrap() > pushdown_limit {
    //                     q.limit = Some(Expr::Value(Value::Number(
    //                         pushdown_limit.to_string(),
    //                         is.clone(),
    //                     )));
    //                 }
    //             }
    //         } else {
    //             q.limit = Some(Expr::Value(Value::Number(
    //                 pushdown_limit.to_string(),
    //                 false,
    //             )));
    //         }
    //     }
    //     ControlFlow::<()>::Continue(())
    // });
    // print!("{}", statements[0]);
    // let ctx = SessionContext::new();
    // let unparsed = match transform_sql_with_ctx(&ctx, mdl, &[], sql).await {
    //     Ok(sql) => println!("{}", sql),
    //     Err(e) => {
    //         eprintln!("Error: {}", e);
    //         return Ok(());
    //     }
    // };
    //     let mut config = ConfigOptions::new();
    //     config.execution.time_zone = Some("+03:00".to_string());
    //     let session_config = SessionConfig::from(config);
    //     let state = SessionStateBuilder::new()
    //         .with_default_features()
    //         .with_config(session_config)
    //         .build();
    //     let ctx = SessionContext::from(state);
    //     ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
    //         "date_diff",
    //         map_data_type("bigint")?,
    //     )));
    //     ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
    //         "year",
    //         map_data_type("bigint")?,
    //     )));
    //     ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
    //         "month",
    //         map_data_type("bigint")?,
    //     )));
    //     ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
    //         "day",
    //         map_data_type("bigint")?,
    //     )));
    //     ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
    //         "age",
    //         map_data_type("interval")?,
    //     )));
    //
    //     // let sqls = fs::read_to_string("wren-example/data/demo_site.sql").unwrap();
    //     let s = r#"
    // select timestamp with time zone '2011-01-01'
    //     "#;
    //     //     for (i, sql) in sqls.lines().enumerate() {
    //     let sqls = vec![s];
    //     for (i, sql) in sqls.into_iter().enumerate() {
    //         if sql.starts_with("--") || sql.is_empty() {
    //             continue;
    //         }
    //         match transform_sql_with_ctx(&ctx, Arc::clone(&mdl), &[], sql).await {
    //             Ok(sql) => {
    //                 println!("{}", sql);
    //             }
    //             Err(e) => {
    //                 println!("{}: {}", i + 1, sql);
    //                 eprintln!("Error: {}", e);
    //                 return Ok(());
    //             }
    //         };
    //     }
    Ok(())
}
