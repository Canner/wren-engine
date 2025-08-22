/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use crate::mdl::dialect::inner_dialect::{get_inner_dialect, InnerDialect};
use crate::mdl::manifest::DataSource;
use datafusion::common::Result;
use datafusion::logical_expr::sqlparser::keywords::ALL_KEYWORDS;
use datafusion::logical_expr::Expr;
use datafusion::sql::sqlparser::ast::{self, WindowFrameBound};
use datafusion::sql::unparser::dialect::{Dialect, IntervalStyle};
use datafusion::sql::unparser::Unparser;
use regex::Regex;

/// WrenDialect is a dialect for Wren engine. Handle the identifier quote style based on the
/// original Datafusion Dialect implementation but with more strict rules.
/// If the identifier isn't lowercase, it will be quoted.
pub struct WrenDialect {
    inner_dialect: Box<dyn InnerDialect>,
}

impl Dialect for WrenDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        if let Some(quote) = self.inner_dialect.identifier_quote_style(identifier) {
            return Some(quote);
        }

        let identifier_regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        if ALL_KEYWORDS.contains(&identifier.to_uppercase().as_str())
            || !identifier_regex.is_match(identifier)
            || non_lowercase(identifier)
        {
            Some('"')
        } else {
            None
        }
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::MySQL
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser<'_>,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if let Some(function) = self
            .inner_dialect
            .scalar_function_to_sql_overrides(unparser, func_name, args)?
        {
            return Ok(Some(function));
        }

        Ok(None)
    }

    fn unnest_as_table_factor(&self) -> bool {
        self.inner_dialect.unnest_as_table_factor()
    }

    fn col_alias_overrides(&self, alias: &str) -> Result<Option<String>> {
        self.inner_dialect.col_alias_overrides(alias)
    }

    fn window_func_support_window_frame(
        &self,
        func_name: &str,
        start_bound: &WindowFrameBound,
        end_bound: &WindowFrameBound,
    ) -> bool {
        self.inner_dialect.window_func_support_window_frame(
            func_name,
            start_bound,
            end_bound,
        )
    }
}

impl Default for WrenDialect {
    fn default() -> Self {
        WrenDialect::new(&DataSource::default())
    }
}

impl WrenDialect {
    pub fn new(data_source: &DataSource) -> Self {
        Self {
            inner_dialect: get_inner_dialect(data_source),
        }
    }
}

fn non_lowercase(sql: &str) -> bool {
    let lowercase = sql.to_lowercase();
    lowercase != sql
}
